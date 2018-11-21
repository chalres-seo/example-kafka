package com.example.kafka.consumer

import java.util
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue, TimeUnit}

import com.example.kafka.metrics.KafkaMetrics
import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

import scala.collection.JavaConversions._

/**
  * kafka consumer client worker class.
  *
  * @param consumerClient consumer client.
  * @param topicName subscrition topic name.
  * @param consumerWorkerExecutorService consumer worker thread pool. (default global thread pool).
  * @tparam K kafka consumer record key deserializer.
  * @tparam V kafka consumer record value deserializer.
  */
class ConsumerWorker[K, V](consumerClient: ConsumerClient[K, V],
                           topicName: String,
                           consumerWorkerExecutorService: Option[ExecutorService] = None) extends LazyLogging {

  private implicit val executionContext: ExecutionContextExecutor =
    consumerWorkerExecutorService match {
      case Some(executorService) =>
        logger.debug("use custom execution context.")
        ExecutionContext.fromExecutorService(executorService)
      case None =>
        logger.debug("use global execution context.")
        ExecutionContext.global
    }

  private val consumerRecordsBuffer: LinkedBlockingQueue[ConsumerRecords[K, V]] =
    new LinkedBlockingQueue[ConsumerRecords[K, V]]()

  /** for thread safe only read */
  @volatile private var workerIsRunning: Boolean = false
  @volatile private var workerIsShutDown: Boolean = false
  @volatile private var workerFuture: Future[Unit] = _
  @volatile private var workerThread: Thread = _

  private val consumerMetrics = consumerClient.getMetrics

  def getBufferSize: Int = {
    logger.debug(s"get buffer size. size ${consumerRecordsBuffer.size()}")
    consumerRecordsBuffer.size()
  }

  def isBufferEmpty: Boolean = {
    logger.debug(s"check buffer is empty. value: ${this.consumerRecordsBuffer.isEmpty}")
    this.consumerRecordsBuffer.isEmpty
  }


  private def setWorkerRunningState(state: Boolean): Unit = {
    logger.debug(s"set worker running state. before: $workerIsRunning, change state: $state")
    workerIsRunning = state
  }

  private def setWorkerShutDownState(state: Boolean): Unit = {
    logger.debug(s"set worker shutdown state. before: $workerIsShutDown, change state: $state")
    workerIsShutDown = state
  }

  private def setWorkerFuture(future: Future[Unit]): Unit = {
    logger.debug(s"set worker future. before: $workerFuture, future: $future")
    workerFuture = future
  }

  private def setWorkerThread(thread: Thread): Unit = {
    logger.debug(s"set worker thread. before: $workerThread, thread: $thread")
    workerThread = thread
  }

  def pollConsumerRecordsFromBuffer(waitTimeMillis: Long = ConsumerWorker.pollBufferWaitMillis): Option[ConsumerRecords[K, V]] = {
    logger.debug("get consumer records from buffer")
    try {
      Option(consumerRecordsBuffer.poll(waitTimeMillis, TimeUnit.MILLISECONDS))
    } catch {
      case e: InterruptedException =>
        logger.warn(s"interrupted polling buffer. msg: ${e.getMessage}")
        None
      case e: Exception =>
        logger.error(s"failed get ConsumerRecords from buffer. msg: ${e.getMessage}", e)
        None
    }
  }

  def getAllConsumerRecordsFromBuffer: Option[Vector[ConsumerRecords[K, V]]] = {
    logger.debug("get all consumer records from buffer")
    val result = try {
      synchronized {
        val bufferSnapShot = if (this.isBufferEmpty) {
          None
        } else {
          Option(consumerRecordsBuffer.toVector)
        }
        consumerRecordsBuffer.clear()
        bufferSnapShot
      }
    } catch {
      case e: InterruptedException =>
        logger.debug(s"interrupted polling buffer. msg: ${e.getMessage}")
        None
      case e: Exception =>
        logger.debug(s"failed get ConsumerRecords from buffer. msg: ${e.getMessage}")
        None
    }

    result
  }

  def start(): Unit = {
    if (workerIsRunning) {
      logger.error("consumer worker already start.")
    } else {
      logger.info(s"consumer worker start.")

      this.setWorkerRunningState(true)
      this.setWorkerFuture(loopTask)
    }

  }

  private def loopTask: Future[Unit] = {
    logger.info("consumer worker task loop start.")
    Future {
      this.setWorkerThread(Thread.currentThread())
      while (workerIsRunning) {
        this.readFromKafka match {
          case Some(consumerRecords) =>
            if (consumerRecords.isEmpty) {
              logger.debug(s"consume record is empty. wait ${ConsumerWorker.defaultWaitMillis} millis for loop task interval.")
              Thread.sleep(ConsumerWorker.defaultWaitMillis)
            } else {
              logger.debug(s"consume record count is ${consumerRecords.count()}")
              if (writeToBuffer(consumerRecords)) {
                consumerClient.offsetCommitAsync()
              } else {
                logger.error("failed put consume record to buffer")
              }
            }
          case None => Unit
        }
      }
      logger.info(s"consumer worker task loop stop. stop subscribe topic: $topicName")
      consumerClient.unsubscribeAllTopic()
      logger.debug(this.getWorkerRunningStateString)
      logger.debug(this.getWorkerFutureAndThreadStateString)
    }
  }

  private def readFromKafka: Option[ConsumerRecords[K, V]] = {
    logger.debug(s"read record from kafka")
    try {
      Option(consumerClient.consumeRecord)
    } catch {
      case e:Exception =>
        logger.error(e.getMessage)
        logger.error(s"failed record read from kafka. assignment: ${consumerClient.getAssignmentTopicPartitionInfo.mkString(", ")}")
        None
    }
  }

  /** caution!! this api cause Inf blocking when putting buffer */
  private def writeToBuffer(consumerRecords: ConsumerRecords[K, V]): Boolean = {
    logger.debug("put consume records to buffer")
    try {
      consumerRecordsBuffer.put(consumerRecords)
      true
    } catch {
      case _:InterruptedException =>
        logger.debug("worker thread is wake up.")
        true
      case e:Exception =>
        logger.error(e.getMessage)
        logger.error(s"failed put consumer records to buffer. records:\n\t${consumerRecords.iterator().mkString("\n\t")}" )
        false
    }
  }

  def stop(): Future[Unit] = {
    Future {
      logger.info("consumer worker stop.")

      // set worker thread shutdown flag. worker is still running but no more subscribe and polling record from kafka.
      this.setWorkerShutDownState(true)


      this.waitCleanUpBuffer()

      // worker is no more running.
      this.setWorkerRunningState(false)
      //consumerClient.unsubscribeAllTopic()

      // wake up blocked worker.
      this.wakeUpWaitWorkerThread()

      this.shutdownHook()


      logger.info("consumer worker stop complete.")
    }
  }

  def forceCleanupAndStop(): Future[Unit] = {
    this.forceCleanUpBuffer()
    this.stop()
  }

  private def waitCleanUpBuffer(): Unit = {
    logger.info(s"clean up buffer. size: ${this.getBufferSize}")
    while (!this.isBufferEmpty) {
      logger.info(s"wait ${ConsumerWorker.remainRecordInBufferWaitMillis} millis for clean up remaining records in buffer.")
      Thread.sleep(ConsumerWorker.remainRecordInBufferWaitMillis)
    }
    logger.info(s"clean up buffer complete. size: ${this.getBufferSize}")
  }

  private def forceCleanUpBuffer(): Unit = {
    logger.info("force clean up buffer.")
    val remainRecords = this.getAllConsumerRecordsFromBuffer

    remainRecords match {
      case Some(records) =>
        logger.info("remain consumer record:\n\t" + records.flatMap(_.iterator()).mkString("\n\t"))
      case None =>
        logger.info("remain consumer record is empty.")
    }
    logger.info("force clean up buffer complete.")
  }

  private def wakeUpWaitWorkerThread(): Unit = {
    logger.debug(s"check thread state and sent wake up signal when thread is waiting.")
    if (workerThread.getState == Thread.State.WAITING) {
      logger.debug(s"sent interrupt signal to worker thread. thread name: ${workerThread.getName}, state: ${workerThread.getState}")
      workerThread.interrupt()
    }
  }

  def close(): Unit = {
    if (workerIsRunning) {
      logger.error("worker is running. can't close kafka consumer.")
    } else {
      consumerClient.close()
    }
  }

  private def shutdownHook(): Unit = {
    logger.info("consumer worker shutdown hook start.")

    // here for before shutdown

    logger.info("consumer worker shutdown hook complete.")
  }

  private def getWorkerFutureAndThreadStateString: String = {
    s"future complete state: ${workerFuture.isCompleted}, thread state: ${workerThread.getName}(${workerThread.getState})"
  }

  private def getWorkerRunningStateString: String = {
    s"worker running state: $workerIsRunning, worker shutdown state: $workerIsShutDown"
  }

  def getKafkaConsumerMetrics: KafkaMetrics = {
    logger.debug("get producer metrics.")
    consumerMetrics
  }
}

object ConsumerWorker extends LazyLogging {
  private lazy val consumerWorkerExecutorService = this.createCustomExecutorService

  private val defaultWaitMillis = 3000L

  private val pollBufferWaitMillis = defaultWaitMillis
  private val offerBufferWaitmillis = defaultWaitMillis
  private val remainRecordInBufferWaitMillis: Long = defaultWaitMillis

  // use when predefine custom commit offset callback
  private lazy val kafkaConsumerOffsetCommitCallback = this.createConsumerOffsetCommitCallBack

  /** constructor */
  def apply[K, V](consumerClient: ConsumerClient[K, V], topicName: String, useGlobalExecutionContext: Boolean): ConsumerWorker[K, V] = {
    logger.debug(s"create consumer worker. subscribe topic name: $topicName, use global execution context: $useGlobalExecutionContext")
    consumerClient.subscribeTopic(topicName)
    new ConsumerWorker(consumerClient, topicName, this.getExecutionContext(useGlobalExecutionContext))
  }

  /** constructor overload */
  def apply[K, V](consumerClient: ConsumerClient[K, V], topicName: String): ConsumerWorker[K, V] = {
    this.apply(consumerClient, topicName, useGlobalExecutionContext = true)
  }

  /** constructor overload */
  def apply[K, V](props: Properties, topicName: String, useGlobalExecutionContext: Boolean): ConsumerWorker[K, V] = {
    this.apply(this.createConsumerClient[K, V](props), topicName, useGlobalExecutionContext)
  }

  /** constructor overload */
  def apply[K, V](props: Properties, topicName: String): ConsumerWorker[K, V] = {
    this.apply(this.createConsumerClient[K, V](props), topicName, useGlobalExecutionContext = true)
  }

  /** consumer client factory api */
  private def createConsumerClient[K, V](props: Properties): ConsumerClient[K, V] = {
    ConsumerClient[K, V](props)
  }

  // use when predefine custom commit offset callback
  private def createConsumerOffsetCommitCallBack: OffsetCommitCallback = {
    new OffsetCommitCallback {
      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
        if (exception == null) {
          logger.debug(s"async commit success, offsets: $offsets")
        } else logger.error(s"commit failed for offsets: $offsets", exception)
      }
    }
  }

  private def getExecutionContext(useGlobalExecutionContext: Boolean): Option[ExecutorService] = {
    if (useGlobalExecutionContext) None else Option(consumerWorkerExecutorService)
  }

  private def createCustomExecutorService: ExecutorService = {
    logger.debug(s"create custom stealing executor service. pool size: ${AppConfig.DEFAULT_EXECUTOR_SERVICE_THREAD_COUNT}")
    Executors.newWorkStealingPool(AppConfig.DEFAULT_EXECUTOR_SERVICE_THREAD_COUNT)
  }

  def terminateCustomExecutorService(): Unit = {
    logger.debug("terminate custom stealing executor service")
    this.consumerWorkerExecutorService.shutdown()
    this.consumerWorkerExecutorService.awaitTermination(30L, TimeUnit.SECONDS)
  }
}

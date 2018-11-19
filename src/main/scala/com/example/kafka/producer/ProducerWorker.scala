package com.example.kafka.producer

import java.util.Properties
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

import com.example.kafka.metrics.KafkaMetrics
import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata, KafkaProducer => IKafkaProducer}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.collection.JavaConverters._

/**
  * kafka producer client worker class.
  * producer pattern worker using linked blocking queue
  * 
  * @see [[ProducerClient]]
  *
  * @param producerClient producer client.
  * @param producerWorkerExecutorService producer worker thread pool. (default using global thread pool). 
  * @tparam K kafka producer record key serializer.
  * @tparam V kafka producer record value serializer.
  */
class ProducerWorker[K, V](producerClient: ProducerClient[K, V],
                           producerWorkerExecutorService: Option[ExecutorService] = None) extends LazyLogging {

  private implicit val executionContext: ExecutionContextExecutor =
    producerWorkerExecutorService match {
      case Some(executorService) =>
        logger.debug("use custom execution context.")
        ExecutionContext.fromExecutorService(executorService)
      case None =>
        logger.debug("use global execution context.")
        ExecutionContext.global
    }

  private val producerRecordBuffer: LinkedBlockingQueue[ProducerRecord[K, V]] =
    new LinkedBlockingQueue[ProducerRecord[K, V]]()

  private val incompleteAsyncProducerRecordCount: AtomicLong = new AtomicLong(0)

  /** thread safe only read */
  @volatile private var workerIsRunning: Boolean = false
  @volatile private var workerIsShutDown: Boolean = false
  @volatile private var workerFuture: Future[Unit] = _
  @volatile private var workerThread: Thread = _

  private val producerMetrics = producerClient.getMetrics
  
  def getBufferSize: Int = {
    logger.debug(s"get buffer size. size ${producerRecordBuffer.size()}")
    producerRecordBuffer.size()
  }

  def bufferIsEmpty: Boolean = {
    logger.debug(s"check buffer is empty. value: ${this.producerRecordBuffer.isEmpty}")
    this.producerRecordBuffer.isEmpty
  }

  def getIncompleteAsyncProduceRecordCount: Long = {
    logger.debug(s"get incomplete async produce record count. ${incompleteAsyncProducerRecordCount.get()}")
    incompleteAsyncProducerRecordCount.get()
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
  private def clearWorkerThreadAndFuture(): Unit = {
    logger.debug("clear worker thread and future.")
    logger.debug(this.getWorkerFutureAndThreadStateString)
    workerThread = null
    workerFuture = null
  }

  def addProducerRecord(record: ProducerRecord[K, V]): Boolean = {
    logger.debug(s"add produce record to buffer.")
    if (workerIsShutDown && workerIsRunning) {
      logger.error(s"worker is not running state or going to shutdown phase, can't add more producer record to buffer.")
      logger.error(s"failed producer record: ${record.toString}")
      false
    } else {
      try {
        producerRecordBuffer.put(record)
        true
      } catch {
        case e: Exception =>
          logger.error("failed putting record to buffer.")
          logger.error(e.getMessage, e)
          logger.error(s"failed record: ${record.toString}")
          false
      }
    }
  }

  def addProducerRecords(records: Vector[ProducerRecord[K, V]]): Boolean = {
    logger.debug(s"add producer records to buffer. record count: ${records.length}")
    if (workerIsShutDown && workerIsRunning) {
      logger.error(s"worker is going to shutdown phase, can't add producer record to buffer. record count: ${records.length}")
      logger.error(s"failed producer records:\n\t" + records.mkString("\t\n"))
      false
    } else {
      // user bulk insert. not thread safe.
      // producerRecordBuffer.addAll(records.asJava)
      val putBufferRecordCount = records.map(this.addProducerRecord).count(_ == true)
      logger.debug(s"The count of records successfully inserted into the buffer. record count: $putBufferRecordCount")
      records.length == putBufferRecordCount
    }
  }

  def start(): Unit = {
    if (workerIsRunning) {
      logger.debug("producer worker already start.")
    } else {
      logger.debug(s"producer worker start.")
//      this.clearWorkerThreadAndFuture()

      this.setWorkerRunningState(true)
      this.setWorkerFuture(loopTask)
    }
  }

  private def loopTask: Future[Unit] = {
    logger.debug("producer worker task loop start.")
    Future {
      this.setWorkerThread(Thread.currentThread())
      while (workerIsRunning) {
        this.readFromBuffer match {
          case Some(produceRecord) =>
            incompleteAsyncProducerRecordCount.incrementAndGet()
            this.sendToKafka(produceRecord)
          case None => Unit
        }
      }
      logger.debug(s"producer worker task loop stop.")
      logger.debug(this.getWorkerRunningStateString)
      logger.debug(this.getWorkerFutureAndThreadStateString)
    }
  }

  private def readFromBuffer: Option[ProducerRecord[K, V]] = {
    logger.debug("take producer record from queue.")
    try {
      Option(producerRecordBuffer.take())
    } catch {
      case _:InterruptedException =>
        logger.debug("worker thread is wake up.")
        None
      case e:Exception =>
        logger.error("failed take producer record from queue.", e)
        None
    }
  }

  private def sendToKafka(produceRecord: ProducerRecord[K, V]): Unit = {
    logger.debug(s"record send to kafka by async. record: $produceRecord")
    try {
      // use custom callback.
      //producerClient.produceRecords(Vector(produceRecord), ProducerWorker.kafkaProducerCallback)
      producerClient.produceRecords(Vector(produceRecord))
    } catch {
      case e:Exception =>
        logger.error(e.getMessage)
        logger.error(s"failed record send to kafka. record: $produceRecord", e)
    } finally {
      incompleteAsyncProducerRecordCount.decrementAndGet()
    }
  }

  def stop(): Future[Unit] = {
    Future {
      logger.debug("producer worker stop.")
      this.setWorkerShutDownState(true)
      this.cleanUpBuffer()

      this.setWorkerRunningState(false)
      
      this.wakeUpWaitWorkerThread()
      producerClient.flush

      this.shutdownHook()
      logger.debug("producer worker stopped.")
    }
  }

  private def cleanUpBuffer(): Unit = {
    logger.debug("clean up remaining records in buffer.")
    while (!producerRecordBuffer.isEmpty) {
      logger.debug(this.getWorkerFutureAndThreadStateString)
      logger.debug(s"wait ${ProducerWorker.remainRecordInBufferWaitMillis} millis for clean up remain record in buffer. buffer size: ${this.getBufferSize}")
      Thread.sleep(ProducerWorker.remainRecordInBufferWaitMillis)
    }
    logger.debug(s"buffer clean up complete. buffer size: ${this.getBufferSize}")
  }
  
  private def cleanUpIncompleteAsyncProducerRecord(): Unit = {
    logger.debug("clean up incomplete producer record.")
    while (!workerFuture.isCompleted || incompleteAsyncProducerRecordCount.get() != 0) {
      try {
        logger.debug(this.getWorkerFutureAndThreadStateString)
        logger.debug(s"wait ${ProducerWorker.incompleteAsyncProducerRecordWaitMillis} millis for clean up incomplete producer record. record count: $incompleteAsyncProducerRecordCount")
        Thread.sleep(ProducerWorker.incompleteAsyncProducerRecordWaitMillis)
        this.wakeUpWaitWorkerThread()
        Thread.sleep(ProducerWorker.incompleteAsyncProducerRecordWaitMillis)
      } catch {
        case e:Exception =>
          logger.error("wait for complete already producer record is interrupted.", e)
      }
    }
    logger.debug(s"incomplete producer record clean up complete. count: $incompleteAsyncProducerRecordCount")
  }

  private def wakeUpWaitWorkerThread(): Unit = {
    logger.debug(s"check thread state and sent wake up signal when thread is waiting.")
    if (workerThread.getState == Thread.State.WAITING) {
      logger.debug(s"sent interrupt signal to worker thread. thread name: ${workerThread.getName}, state: ${workerThread.getState}")
      workerThread.interrupt()
    }
  }

  def close(): Unit = {
    logger.debug("producer close")
    if (workerIsRunning) {
      logger.error("worker is running. can't close producer.")
    } else {
      producerClient.close()
      logger.debug("producer close complete.")
    }
  }
  
  private def shutdownHook(): Unit = {
    logger.debug("producer worker shutdown hook start.")
    this.cleanUpIncompleteAsyncProducerRecord()
    logger.debug("producer worker shutdown complete.")
  }
  
  private def getWorkerFutureAndThreadStateString: String = {
    s"future complete state: ${workerFuture.isCompleted}, thread state: ${workerThread.getName}(${workerThread.getState})"
  }

  private def getWorkerRunningStateString: String = {
    s"worker running state: $workerIsRunning, worker shutdown state: $workerIsShutDown"
  }

  def getKafkaProducerMetrics: KafkaMetrics = {
    logger.debug("get producer metrics.")
    producerMetrics
  }
}

object ProducerWorker extends LazyLogging {
  private lazy val producerWorkerExecutorService = this.createCustomExecutorService

  private val incompleteAsyncProducerRecordWaitMillis: Long = 3000L
  private val remainRecordInBufferWaitMillis: Long = 1000L

  // use custom callback.
  private lazy val kafkaProducerCallback = this.createProducerCallBack

  /** producer worker constructor */
  def apply[K, V](producerClient: ProducerClient[K, V],
                  props: Properties,
                  useGlobalExecutionContext: Boolean): ProducerWorker[K, V] = {
    logger.debug(s"create producer worker. use global execution context: $useGlobalExecutionContext")
    new ProducerWorker(producerClient, if (useGlobalExecutionContext) None else Option(producerWorkerExecutorService))
  }

  def apply[K, V](producerClient: ProducerClient[K, V], props: Properties): ProducerWorker[K, V] = {
    this.apply(producerClient, props, useGlobalExecutionContext = true)
  }

  def apply[K, V](props: Properties, useGlobalExecutionContext: Boolean): ProducerWorker[K, V] = {
    this.apply(this.createProducerClient[K, V](props), props, useGlobalExecutionContext)
  }

  def apply[K, V](props: Properties): ProducerWorker[K, V] = {
    this.apply(this.createProducerClient[K, V](props), props, useGlobalExecutionContext = true)
  }

  /** producer client factory api */
  private def createProducerClient[K, V](props: Properties): ProducerClient[K, V] = {
    ProducerClient[K, V](props)
  }

  private def recordMetadataToMap(recordMetadata: RecordMetadata): Map[String, Any] = {
    Map(
      "offset" -> recordMetadata.offset(),
      "partition" -> recordMetadata.partition(),
      "timestamp" -> recordMetadata.timestamp(),
      "topic" -> recordMetadata.topic()
    )
  }

  // use custom callback.
  private def createProducerCallBack: Callback = new Callback() {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) {
        logger.debug(s"succeed send record. metadata: " +
          s"${ProducerWorker.recordMetadataToMap(metadata)}")
      } else {
        logger.error(s"failed send record. metadata: " +
          s"${ProducerWorker.recordMetadataToMap(metadata)}", exception)
      }
    }
  }

  private def createCustomExecutorService: ExecutorService = {
    logger.debug(s"create custom stealing executor service. pool size: ${AppConfig.DEFAULT_EXECUTOR_SERVICE_THREAD_COUNT}")
    Executors.newWorkStealingPool(AppConfig.DEFAULT_EXECUTOR_SERVICE_THREAD_COUNT)
  }

  def terminateCustomExecutorService(): Unit = {
    logger.debug("terminate custom stealing executor service")
    this.producerWorkerExecutorService.shutdown()
    this.producerWorkerExecutorService.awaitTermination(30L, TimeUnit.SECONDS)
  }
}
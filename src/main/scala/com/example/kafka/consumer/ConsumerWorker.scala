//package com.example.kafka.consumer
//
//import java.time.Duration
//import java.util
//import java.util.Properties
//import java.util.concurrent.atomic.AtomicBoolean
//import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue, TimeUnit}
//
//import com.example.kafka.metrics.KafkaMetrics
//import com.example.utils.AppConfig
//import com.typesafe.scalalogging.LazyLogging
//import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetAndMetadata, OffsetCommitCallback, OffsetResetStrategy, Consumer => IKafkaConsumer}
//import org.apache.kafka.common.serialization.Deserializer
//import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
//
//import scala.collection.mutable.ListBuffer
//import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
//import scala.concurrent.duration.TimeUnit
//import scala.collection.JavaConversions._
//
//class ConsumerWorker[K, V](consumerClient: ConsumerClient[K, V],
//                           props: Properties,
//                           topicName: String,
//                           queueConsumerExecutorService: Option[ExecutorService] = None) extends LazyLogging {
//
//  //type ConsumerHandlerType = ConsumerRecords[K, V] => Future[Unit]
//
//  private implicit val queueConsumerExecutionContext: ExecutionContextExecutor =
//    queueConsumerExecutorService match {
//      case Some(executorService) =>
//        logger.debug("use custom execution context.")
//        ExecutionContext.fromExecutorService(executorService)
//      case None =>
//        logger.debug("use global execution context.")
//        ExecutionContext.global
//    }
//
//  private val consumerRecordBuffer: LinkedBlockingQueue[ConsumerRecords[K, V]] =
//    new LinkedBlockingQueue[ConsumerRecords[K, V]]()
//
//  private val consumerMetrics = consumerClient.getMetrics
//
//  private val workerIsRunning: AtomicBoolean = new AtomicBoolean(false)
//  private val workerIsShutDown: AtomicBoolean = new AtomicBoolean(false)
//
//  private val workerFuture = new Array[Future[Unit]](1)
//  private val workerThread = new Array[Thread](1)
//
//  private val tempForDrain: ListBuffer[ConsumerRecords[K, V]] = new ListBuffer[ConsumerRecords[K, V]]
//
//  private def setWorkerThread(thread: Thread): Unit = {
//    logger.debug(s"set worker thread. thread name: ${thread.getName}")
//    workerThread(0) = thread
//  }
//
//  private def setWorkerFuture(future: Future[Unit]): Unit = {
//    logger.debug(s"set worker future.")
//    workerFuture(0) = future
//  }
//
//  private def clearWorkerThreadAndFuture(): Unit = {
//    logger.debug("clear worker thread and future.")
//    workerThread(0) = null
//    workerFuture(0) = null
//  }
//
//  private def getWorkerThread: Thread = workerThread(0)
//  private def getWorkerFuture: Future[Unit] = workerFuture(0)
//
//  private def loggingThreadAndFutureState(): Unit = {
//    logger.debug(s"future complete state: ${this.getWorkerFuture.isCompleted}")
//    logger.debug(s"thread state: ${this.getWorkerThread.getName}(${this.getWorkerThread.getState})")
//  }
//
//  def getKafkaConsumerMetrics: KafkaMetrics = consumerMetrics
//
//  def getBufferSize: Int = {
//    logger.debug(s"get buffer size. size ${consumerRecordBuffer.size()}")
//    consumerRecordBuffer.size()
//  }
//
//  def bufferIsEmpty: Boolean = this.getBufferSize == 0
//
//  def getConsumerRecords: Vector[ConsumerRecords[K, V]] = {
//    synchronized {
//      tempForDrain.clear()
//      this.consumerRecordBuffer.drainTo(tempForDrain, Int.MaxValue)
//      tempForDrain.toVector
//    }
//  }
//
//  def getConsumerRecord: ConsumerRecords[K, V] = {
//    this.consumerRecordBuffer.take()
//  }
//
//  def getConsumerRecord(unit: Long, timeUnit: TimeUnit): ConsumerRecords[K, V] = {
//    this.consumerRecordBuffer.poll(unit, timeUnit)
//  }
//
//  def start(): Unit = {
//    if (workerIsRunning.get) {
//      logger.debug("consumer worker already start.")
//    } else {
//      logger.debug(s"consumer worker start.")
//      this.clearWorkerThreadAndFuture()
//      workerIsRunning.set(true)
//
//      this.setWorkerFuture(loopTask)
//    }
//  }
//
//  private def loopTask: Future[Unit] = {
//    logger.debug("consumer worker task loop start.")
//
//    Future {
//      this.setWorkerThread(Thread.currentThread())
//      while (workerIsRunning.get) {
//        logger.debug("record read from kafka")
//        this.readFromKafka match {
//          case Some(consumerRecords) =>
//            if (consumerRecords.count == 0) {
//              logger.debug("consume record count is 0. wait 1 sec.")
//              Thread.sleep(1000)
//            } else {
//              if (writeToBuffer(consumerRecords).isDefined) {
//                logger.debug(s"consume record count is ${consumerRecords.count()}")
//                kafkaConsumer.commitAsync(ConsumerWorker.kafkaConsumerCommitCallback)
//                logger.debug("offset commit async.")
//              }
//            }
//          case None => Unit
//        }
//      }
//      logger.debug(s"consumer worker task loop stop. worker is running: ${workerIsRunning.get()}")
//    }
//  }
//
//  private def readFromKafka: Option[ConsumerRecords[K, V]] = {
//    logger.debug(s"record read from kafka")
//
//    try {
//      Option(kafkaConsumer.poll(Duration.ofMillis(1000)))
//    } catch {
//      case e:Exception =>
//        logger.error(e.getMessage)
//        logger.error(s"failed record read from kafka. assignment: ${kafkaConsumer.assignment().mkString(", ")}")
//        None
//    }
//  }
//
//  private def writeToBuffer(consumerRecords: ConsumerRecords[K, V]): Option[Unit] = {
//    logger.debug("put consumer record to queue.")
//    try {
//      Option(consumerRecordBuffer.put(consumerRecords))
//    } catch {
//      case _:InterruptedException =>
//        logger.debug("worker thread is wake up.")
//        None
//      case e:Exception =>
//        logger.error("failed put consumer record to queue.", e)
//        consumerRecords.foreach(record => logger.error(record.toString))
//        None
//    }
//  }
//
//  def stop(): Future[Unit] = {
//    Future {
//      logger.debug("consumer worker stop.")
//
//      logger.debug(s"set consumer worker running state to false. before running state: ${workerIsRunning.get()}")
//      if (workerIsRunning.get) workerIsRunning.set(false)
//      this.wakeUpWaitWorkerThread()
//
//      logger.debug(s"set consumer worker shutdown state to true. before shutdown state: ${workerIsShutDown.get()}")
//      if (!workerIsShutDown.get()) workerIsShutDown.set(true)
//
//      while (!consumerRecordBuffer.isEmpty) {
//        val remainConsumerRecords: ListBuffer[ConsumerRecords[K, V]] = new ListBuffer[ConsumerRecords[K, V]]
//        consumerRecordBuffer.drainTo(remainConsumerRecords, Int.MaxValue)
//
//        logger.error(s"remain consume record: ${remainConsumerRecords.length}")
//        remainConsumerRecords.flatMap(_.iterator()).foreach(record => logger.error(record.toString))
//
//        logger.debug(s"wait for ${ConsumerWorker.remainRecordInBufferWaitMillis} millis remain record in buffer. buffer size: ${this.getBufferSize}")
//        this.loggingThreadAndFutureState()
//        Thread.sleep(ConsumerWorker.remainRecordInBufferWaitMillis)
//      }
//      logger.debug(s"buffer clean up complete. size: ${this.getBufferSize}")
//
//      this.shutdownHook()
//      logger.debug("consumer worker stopped.")
//    }
//  }
//
//  def close(): Unit = {
//    logger.debug("consumer close")
//    if (workerIsRunning.get()) {
//      logger.error("worker is running. can't close consumer.")
//    } else {
//      this.kafkaConsumer.close()
//      logger.debug("consumer close complete.")
//    }
//  }
//
//
//
//  private def wakeUpWaitWorkerThread(): Unit = {
//    logger.debug(s"check thread state and sent wake up signal when thread is waiting.")
//    if (this.getWorkerThread.getState == Thread.State.WAITING) {
//      logger.debug(s"sent interrupt signal to worker thread. thread name: ${this.getWorkerThread.getName}, state: ${this.getWorkerThread.getState}")
//      this.getWorkerThread.interrupt()
//    }
//  }
//
//  private def shutdownHook(): Unit = {
//    logger.debug("consumer worker shutdown hook start.")
//    while (!this.getWorkerFuture.isCompleted) {
//      try {
//        this.loggingThreadAndFutureState()
//        this.wakeUpWaitWorkerThread()
//        Thread.sleep(ConsumerWorker.remainRecordInBufferWaitMillis)
//      } catch {
//        case e:Exception =>
//          logger.error("wait for complete already consumer record is interrupted.", e)
//      }
//    }
//    this.kafkaConsumer.commitSync()
//    logger.debug("consumer worker shutdown complete.")
//  }
//
//}
//
//object ConsumerWorker extends LazyLogging {
//  private lazy val consumerWorkerExecutorService = this.createCustomExecutorService
//
//  private val remainRecordInBufferWaitMillis: Long = 1000L
//
//  // use custom commit offset callback
//  private lazy val kafkaConsumerOffsetCommitCallback = this.createConsumerOffsetCommitCallBack
//
//  /** producer worker constructor */
//  def apply[K, V](consumerClient: ConsumerClient[K, V], props: Properties, topicName: String, useGlobalExecutionContext: Boolean): ConsumerWorker[K, V] = {
//    logger.debug(s"create consumer worker. subscribe topic name: $topicName, use global execution context: $useGlobalExecutionContext")
//    consumerClient.subscribeTopic(topicName)
//    new ConsumerWorker(consumerClient, props, topicName, this.getExecutionContext(useGlobalExecutionContext))
//  }
//
//  def apply[K, V](consumerClient: ConsumerClient[K, V], props: Properties, topicName: String): ConsumerWorker[K, V] = {
//    this.apply(consumerClient, props, topicName, useGlobalExecutionContext = true)
//  }
//
//  def apply[K, V](props: Properties, topicName: String, useGlobalExecutionContext: Boolean): ConsumerWorker[K, V] = {
//    this.apply(this.createConsumerClient[K, V](props), props, topicName, useGlobalExecutionContext)
//  }
//
//  def apply[K, V](props: Properties, topicName: String): ConsumerWorker[K, V] = {
//    this.apply(this.createConsumerClient[K, V](props), props, topicName, useGlobalExecutionContext = true)
//  }
//
//  /** consumer client factory api */
//  private def createConsumerClient[K, V](props: Properties): ConsumerClient[K, V] = {
//    ConsumerClient[K, V](props)
//  }
//
//  private def getExecutionContext(useGlobalExecutionContext: Boolean): Option[ExecutorService] = {
//    if (useGlobalExecutionContext) None else Option(consumerWorkerExecutorService)
//  }
//
//  private def createConsumerOffsetCommitCallBack: OffsetCommitCallback = {
//    new OffsetCommitCallback {
//      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
//        if (exception == null) {
//          logger.debug(s"async commit success, offsets: $offsets")
//        } else logger.error(s"commit failed for offsets: $offsets", exception)
//      }
//    }
//  }
//
//  private def createCustomExecutorService: ExecutorService = {
//    logger.debug(s"create custom stealing executor service. pool size: ${AppConfig.DEFAULT_EXECUTOR_SERVICE_THREAD_COUNT}")
//    Executors.newWorkStealingPool(AppConfig.DEFAULT_EXECUTOR_SERVICE_THREAD_COUNT)
//  }
//
//  def terminateCustomExecutorService(): Unit = {
//    logger.debug("terminate custom stealing executor service")
//    this.consumerWorkerExecutorService.shutdown()
//    this.consumerWorkerExecutorService.awaitTermination(30L, TimeUnit.SECONDS)
//  }
//}

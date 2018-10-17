package com.example.kafka.producer

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.example.kafka.metrics.KafkaMetrics
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata, Producer => IKafkaProducer}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

class ProducerWorker[K, V](kafkaProducer: IKafkaProducer[K, V],
                           producerWorkerExecutorService: Option[ExecutorService] = None) extends LazyLogging {

  private implicit val executionContext: ExecutionContextExecutor = producerWorkerExecutorService match {
    case Some(executorService) =>
      logger.info("use custom execution context.")
      ExecutionContext.fromExecutorService(executorService)
    case None =>
      logger.info("use global execution context.")
      ExecutionContext.global
  }

  private val producerRecordBuffer: LinkedBlockingQueue[ProducerRecord[K, V]] =
    new LinkedBlockingQueue[ProducerRecord[K, V]]()

  private val incompleteAsyncProduceRecordCount: AtomicLong = new AtomicLong(0)

  private val kafkaProducerMetrics = kafkaProducer.metrics()
  private val producerMetrics = KafkaMetrics(kafkaProducerMetrics)

  private val workerIsRunning: AtomicBoolean = new AtomicBoolean(false)
  private val workerIsShutDown: AtomicBoolean = new AtomicBoolean(false)

  private val workerFutureThreadList = ListBuffer.empty[Thread]
  private val workerFutureList = ListBuffer.empty[Future[Unit]]

  private def setWorkerThread(thread: Thread): Unit = {
    logger.info(s"set worker thread. thread name: ${thread.getName}")
    workerFutureThreadList.append(thread)
  }

  private def setWorkerFuture(future: Future[Unit]): Unit = {
    logger.info(s"set worker future.")
    workerFutureList.append(future)
  }

  private def clearWorkerThreadAndFuture(): Unit = {
    logger.info("clear worker thread and future.")
    workerFutureList.clear()
    workerFutureThreadList.clear()
  }

  def getIncompleteAsyncProduceRecordCount: Long = incompleteAsyncProduceRecordCount.get()

  def getKafkaProducerMetrics: KafkaMetrics = producerMetrics

  def getBufferSize: Int = {
    logger.debug(s"get buffer size. size ${producerRecordBuffer.size()}")
    producerRecordBuffer.size()
  }

  def bufferIsEmpty: Boolean = this.producerRecordBuffer.isEmpty

  def getThreadAndFutureState: Vector[String] = {
    workerFutureThreadList.zip(workerFutureList).map { case (thread, future) =>
      s"future complete state: ${future.isCompleted}, ${thread.getName}: ${thread.getState}"
    }.toVector
  }

  def addProduceRecord(record: ProducerRecord[K, V]): Unit = {
    logger.trace(s"add produce record to buffer. $record")
    if (workerIsShutDown.get) {
      logger.error(s"worker is going to shutdown phase, can't add producer record to buffer. record: $record")
    } else {
      producerRecordBuffer.put(record)
    }
  }

  def addProduceRecords(records: Vector[ProducerRecord[K, V]]): Unit = {
    logger.trace(s"add produce records to buffer. record count: ${records.length}")
    if (workerIsShutDown.get) {
      logger.error(s"worker is going to shutdown phase, can't add producer record to buffer." +
        s" record count: ${records.length}")
      records.foreach(record => logger.error(record.toString))
    } else {
      for (record <- records) {
        producerRecordBuffer.put(record)
      }
    }
  }

  def start(): Unit = {
    this.start(1)
  }

  def start(parallelLevel: Int): Unit = {
    if (workerIsRunning.get) {
      logger.info("producer worker already start.")
    } else {
      logger.info(s"producer worker start. parallel level: $parallelLevel")
      this.clearWorkerThreadAndFuture()
      workerIsRunning.set(true)

      val maxParallelLevel = Runtime.getRuntime.availableProcessors() -1

      if (parallelLevel > maxParallelLevel) {
        logger.info(s"parallel level is exceed available processor count. " +
          s"parallel level reset available processor count: $maxParallelLevel")
        (1 to maxParallelLevel).foreach(_ => this.setWorkerFuture(loopTask))
      } else {
        (1 to parallelLevel).foreach(_ => this.setWorkerFuture(loopTask))
      }
    }
  }

  private def loopTask: Future[Unit] = {
    logger.info("producer worker task loop start.")

    Future {
      this.setWorkerThread(Thread.currentThread())
      while (workerIsRunning.get) {
        this.readFromBuffer match {
          case Some(produceRecord) =>
            incompleteAsyncProduceRecordCount.incrementAndGet()
            this.writeToKafka(produceRecord)
          case None => Unit
        }
      }
      logger.info(s"producer worker task loop stop. worker is running: ${workerIsRunning.get()}")
    }
  }

  private def readFromBuffer: Option[ProducerRecord[K, V]] = {
    logger.trace("take producer record from queue.")
    try {
      Option(producerRecordBuffer.take())
    } catch {
      case _:InterruptedException =>
        logger.info("worker thread is wake up.")
        None
      case e:Exception =>
        logger.error("failed take producer record from queue.", e)
        None
    }
  }

  private def writeToKafka(produceRecord: ProducerRecord[K, V]) = {
    logger.trace(s"record send to kafka by async. record: $produceRecord")

    try {
      kafkaProducer.send(produceRecord, ProducerWorker.getProduceCallBack)
    } catch {
      case e:SerializationException =>
        logger.error(e.getMessage)
        logger.error(s"failed record send to kafka. record: $produceRecord")
      case e:Exception =>
        logger.error(s"failed unknown exception record send to kafka unknown exception. record: $produceRecord", e)
    } finally {
      incompleteAsyncProduceRecordCount.decrementAndGet()
    }
  }

  def stop(): Future[Unit] = {
    Future {
      logger.info("producer worker stop.")
      logger.info(s"set producer worker shutdown state to true. before shutdown state: ${workerIsShutDown.get()}")
      if (!workerIsShutDown.get()) workerIsShutDown.set(true)

      while (!producerRecordBuffer.isEmpty) {
        logger.info(s"wait for ${ProducerWorker.remainRecordInBufferWaitMillis} millis remain record in buffer." +
          s" buffer size: ${this.getBufferSize}")
        logger.info(this.getThreadAndFutureState.mkString(", "))
        Thread.sleep(ProducerWorker.remainRecordInBufferWaitMillis)
      }
      logger.info(s"buffer clean up complete. size: ${this.getBufferSize}")

      logger.info(s"set producer worker running state to false. before running state: ${workerIsRunning.get()}")
      if (workerIsRunning.get) workerIsRunning.set(false)

      logger.info("flush kafka producer.")
      this.wakeUpAllWaitWorkerThread()
      this.kafkaProducer.flush()

      this.shutdownHook()
      logger.info("producer worker stopped.")
    }
  }

  def close(): Unit = {
    this.kafkaProducer.close()
  }

  private def wakeUpAllWaitWorkerThread(): Unit = {
    logger.info(s"check all thread state and sent wake up signal when thread is waiting.")
    for {
      thread <- workerFutureThreadList
      if thread.getState == Thread.State.WAITING
    } {
      logger.info(s"sent interrupt signal to worker thread. thread name: ${thread.getName}, state: ${thread.getState}")
      thread.interrupt()
    }
  }

  private def shutdownHook(): Unit = {
    logger.info("producer worker shutdown start.")
    while (!workerFutureList.forall(_.isCompleted) || incompleteAsyncProduceRecordCount.get() != 0) {
      try {
        logger.info("future status. :" + workerFutureList.mkString(", "))
        logger.info("thread status. :" + workerFutureThreadList.map(t => t.getName + "::" + t.getState).mkString(", "))
        logger.info(s"wait ${ProducerWorker.incompleteAsyncProducerRecordWaitMillis} millis for incomplete producer record. " +
          s"record count: $incompleteAsyncProduceRecordCount")
        Thread.sleep(ProducerWorker.incompleteAsyncProducerRecordWaitMillis)
        this.wakeUpAllWaitWorkerThread()
        Thread.sleep(1000)
      } catch {
        case e:Exception =>
          logger.error("wait for complete already producer record is interrupted.", e)
      }
    }
    logger.info("producer worker shutdown complete.")
    logger.info(s"incomplete producer record count: $incompleteAsyncProduceRecordCount")
  }

}

object ProducerWorker extends LazyLogging {
  private val defaultExecutorServiceThreadCount = Runtime.getRuntime.availableProcessors() * 2
  private lazy val producerWorkerExecutorService = this.createCustomExecutorService

  private val incompleteAsyncProducerRecordWaitMillis: Long = 3000L
  private val remainRecordInBufferWaitMillis: Long = 1000L

  private lazy val kafkaProducerCallback = this.createProducerCallBack

  def apply[K, V](keySerializer: Serializer[K],
                  valueSerializer: Serializer[V],
                  useGlobalExecutionContext: Boolean = true): ProducerWorker[K, V] = {
    new ProducerWorker[K, V](
      KafkaProducerFactory.createProducer(keySerializer: Serializer[K], valueSerializer: Serializer[V]),
      if (useGlobalExecutionContext) None else Option(producerWorkerExecutorService))
  }

  def mock[K, V](keySerializer: Serializer[K], valueSerializer: Serializer[V]): ProducerWorker[K, V] = {
    new ProducerWorker(KafkaProducerFactory.createMockProducer(keySerializer, valueSerializer))
  }

  private def getProduceCallBack: Callback = this.kafkaProducerCallback

  private def recordMetadataToMap(recordMetadata: RecordMetadata): Map[String, Any] = {
    Map(
      "offset" -> recordMetadata.offset(),
      "partition" -> recordMetadata.partition(),
      "timestamp" -> recordMetadata.timestamp(),
      "topic" -> recordMetadata.topic()
    )
  }

  private def createProducerCallBack: Callback = new Callback() {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) {
        logger.trace(s"succeed send record. metadata: " +
          s"${ProducerWorker.recordMetadataToMap(metadata)}")
      } else {
        logger.error(s"failed send record. metadata: " +
          s"${ProducerWorker.recordMetadataToMap(metadata)}", exception)
      }
    }
  }

  private def createCustomExecutorService: ExecutorService = {
    logger.info(s"create custom stealing executor service. pool size: $defaultExecutorServiceThreadCount")
    Executors.newWorkStealingPool(defaultExecutorServiceThreadCount)
  }
}
package com.example.kafka.consumer

import java.time.Duration
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue}

import com.example.kafka.metrics.KafkaMetrics
import com.example.kafka.producer.ProducerWorker
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetResetStrategy, Consumer => IKafkaConsumer}
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.{Metric, MetricName}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.TimeUnit

import scala.collection.JavaConversions._

// TODO: consumer worker loop logic
class ConsumerWorker[K, V](kafkaConsumer: IKafkaConsumer[K, V],
                           queueConsumerExecutorService: Option[ExecutorService] = None) extends LazyLogging {

  private implicit val queueConsumerExecutionContext: ExecutionContextExecutor = queueConsumerExecutorService match {
    case Some(executorService) =>
      logger.info("use custom execution context.")
      ExecutionContext.fromExecutorService(executorService)
    case None =>
      logger.info("use global execution context.")
      ExecutionContext.global
  }

  private val consumerRecordBuffer: LinkedBlockingQueue[ConsumerRecords[K, V]] =
    new LinkedBlockingQueue[ConsumerRecords[K, V]]()

  private val drainBuffer = new ListBuffer[ConsumerRecords[K, V]]()

  private val kafkaConsumerMetrics: util.Map[MetricName, _ <: Metric] = kafkaConsumer.metrics()
  private val consumerMetrics = KafkaMetrics(kafkaConsumerMetrics)

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

  def getKafkaConsumer: IKafkaConsumer[K, V] = kafkaConsumer

  def getKafkaConsumerMetrics: KafkaMetrics = consumerMetrics

  def getThreadAndFutureState: immutable.IndexedSeq[String] = {
    for (index <- workerFutureThreadList.indices) yield {
      s"future complete state: ${workerFutureList(index).isCompleted}, " +
        s"${workerFutureThreadList(index).getName}: ${workerFutureThreadList(index).getState}"
    }
  }

  def getBufferSize: Int = {
    logger.debug(s"get buffer size. size ${consumerRecordBuffer.size()}")
    consumerRecordBuffer.size()
  }

  def bufferIsEmpty: Boolean = this.getBufferSize == 0

  def getConsumerRecords(): ConsumerRecords[K, V] = {
    this.consumerRecordBuffer.take()
  }

  def getConsumerRecords(unit: Long, timeUnit: TimeUnit): ConsumerRecords[K, V] = {
    this.consumerRecordBuffer.poll(unit, timeUnit)
  }

  def start(): Unit = {
    this.start(1)
  }

  def start(parallelLevel: Int): Unit = {
    if (workerIsRunning.get) {
      logger.info("consumer worker already start.")
    } else {
      logger.info(s"consumer worker start. parallel level: $parallelLevel")
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
    logger.info("consumer worker task loop start.")

    Future {
      this.setWorkerThread(Thread.currentThread())
      while (workerIsRunning.get) {
        this.readFromKafka match {
          case Some(consumerRecords) =>
            //this.consumerRecordBuffer.put(consumerRecords)
          case None => Unit
        }
        Thread.sleep(1000)
      }
      logger.info(s"producer worker task loop stop. worker is running: ${workerIsRunning.get()}")
    }
  }

  private def readFromKafka: Option[ConsumerRecords[K, V]] = {
    logger.trace(s"record read from kafka")

    try {

      val result = kafkaConsumer.poll(Duration.ofSeconds(10))
      kafkaConsumer.poll(Duration.ofSeconds(3)).iterator().foreach(println)
      println("a")
      Option(result)
    } catch {
      case e:Exception =>
        logger.error(e.getMessage)
        logger.error(s"failed record read from kafka. assignment: ${kafkaConsumer.assignment().mkString(", ")}")
        None
    }
  }

  def stop(): Future[Unit] = {
    Future {
      logger.info("consumer worker stop.")

      logger.info(s"set consumer worker running state to false. before running state: ${workerIsRunning.get()}")
      if (workerIsRunning.get) workerIsRunning.set(false)
      this.wakeUpAllWaitWorkerThread()

      logger.info(s"set consumer worker shutdown state to true. before shutdown state: ${workerIsShutDown.get()}")
      if (!workerIsShutDown.get()) workerIsShutDown.set(true)

      while (!consumerRecordBuffer.isEmpty) {
        println(consumerRecordBuffer.isEmpty)
        val remainConsumerRecords = new ListBuffer[ConsumerRecords[K, V]]
        consumerRecordBuffer.drainTo(remainConsumerRecords)

        logger.error("remain consume record: \n" + remainConsumerRecords.mkString("\n"))

        logger.info(s"wait for ${ConsumerWorker.remainRecordInBufferWaitMillis} millis remain record in buffer." +
          s" buffer size: ${this.getBufferSize}")
        logger.info(this.getThreadAndFutureState.mkString(", "))
        Thread.sleep(ConsumerWorker.remainRecordInBufferWaitMillis)
      }
      logger.info(s"buffer clean up complete. size: ${this.getBufferSize}")

      logger.info("sync kafka consumer.")
      this.kafkaConsumer.commitSync()

      this.shutdownHook()
      logger.info("consumer worker stopped.")
    }
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
    logger.info("consumer worker shutdown start.")
    while (!workerFutureList.forall(_.isCompleted)) {
      try {
        logger.info("future status. :" + workerFutureList.mkString(", "))
        logger.info("thread status. :" + workerFutureThreadList.map(t => t.getName + "::" + t.getState).mkString(", "))
        this.wakeUpAllWaitWorkerThread()
      } catch {
        case e:Exception =>
          logger.error("wait for complete already producer record is interrupted.", e)
      }
    }
    logger.info("consumer worker shutdown complete.")
  }

}

object ConsumerWorker extends LazyLogging {
  private val defaultExecutorServiceThreadCount = Runtime.getRuntime.availableProcessors() * 2

  private lazy val consumerWorkerExecutorService = this.createCustomExecutorService

  private val remainRecordInBufferWaitMillis: Long = 1000L

  def apply[K, V](keyDeserializer: Deserializer[K],
                  valueDeserializer: Deserializer[V],
                  useGlobalExecutionContext: Boolean = true): ConsumerWorker[K, V] = {
    new ConsumerWorker[K, V](KafkaConsumerFactory.createConsumer(keyDeserializer, valueDeserializer),
      if (useGlobalExecutionContext) None else Option(consumerWorkerExecutorService))
  }

  def mock[K, V](offsetResetStrategy: OffsetResetStrategy = OffsetResetStrategy.LATEST): ConsumerWorker[K, V] = {
    new ConsumerWorker[K, V](KafkaConsumerFactory.createMockConsumer(offsetResetStrategy))
  }

  private def createCustomExecutorService: ExecutorService = {
    logger.info(s"create custom stealing executor service. pool size: $defaultExecutorServiceThreadCount")
    Executors.newWorkStealingPool(defaultExecutorServiceThreadCount)
  }

}

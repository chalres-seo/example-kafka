package com.example.kafka.producer

import java.util.concurrent.Future

import ch.qos.logback.classic.{Level, Logger}
import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.{Assert, Test}
import org.hamcrest.CoreMatchers._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class TestProducerWorker extends LazyLogging {
  private val testTopicName: String = "test-producer"
  private val testPerfTopicName: String = "test-producer-perf"

  private val testRecordSet10: Vector[ProducerRecord[String, String]] = (1 to 10)
    .map(index => new ProducerRecord[String, String](testTopicName, s"key-$index", s"value-$index"))
    .toVector

  private val testRecordSet1000000: Vector[ProducerRecord[String, String]] = (1 to 1000000)
    .map(index => new ProducerRecord[String, String](testPerfTopicName, s"key-$index", s"value-$index"))
    .toVector

  @Test
  def testProducer(): Unit = {
    logger.info("create kafka producer.")
    val kafkaProducer: KafkaProducer[String, String] =
      new KafkaProducer(AppConfig.getKafkaProducerProps, new StringSerializer(), new StringSerializer())

    val futures: Vector[Future[RecordMetadata]] = testRecordSet10.map(kafkaProducer.send)

    while(!futures.forall(_.isDone)) { Thread.sleep(1000) }

    futures.foreach { f =>
      val metadata = f.get()
      logger.debug(s"succeed send record. metadata: "
        + "topic: " + metadata.topic() + ", "
        + "partition: " + metadata.partition() + ", "
        + "offset: " + metadata.offset() + ", "
        + "timestamp: " + metadata.timestamp() + ", "
        + "serialized key size: " + metadata.serializedKeySize() + ", "
        + "serialized value size: " + metadata.serializedValueSize())
    }
    kafkaProducer.flush()
    kafkaProducer.close()
  }

  @Test
  def testProducerWithCallback(): Unit = {
    logger.info("create kafka producer.")
    val kafkaProducer: KafkaProducer[String, String] =
      new KafkaProducer(AppConfig.getKafkaProducerProps, new StringSerializer(), new StringSerializer())

    val callback = new Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) {
          logger.debug(s"succeed send record. metadata: "
            + "topic: " + metadata.topic() + ", "
            + "partition: " + metadata.partition() + ", "
            + "offset: " + metadata.offset() + ", "
            + "timestamp: " + metadata.timestamp() + ", "
            + "serialized key size: " + metadata.serializedKeySize() + ", "
            + "serialized value size: " + metadata.serializedValueSize())
        } else {
          logger.error(s"failed send record. metadata: $metadata", exception)

        }
      }
    }

    val futures: Vector[Future[RecordMetadata]] = testRecordSet10.map(kafkaProducer.send(_, callback))

    while(!futures.forall(_.isDone)) { Thread.sleep(1000) }

    futures.foreach { f =>
      val metadata = f.get()
      logger.debug(s"succeed send record. metadata: "
        + "topic: " + metadata.topic() + ", "
        + "partition: " + metadata.partition() + ", "
        + "offset: " + metadata.offset() + ", "
        + "timestamp: " + metadata.timestamp() + ", "
        + "serialized key size: " + metadata.serializedKeySize() + ", "
        + "serialized value size: " + metadata.serializedValueSize())
    }

    kafkaProducer.flush()
    kafkaProducer.close()
  }

  @Test
  def testProducerWorker(): Unit = {
    logger.info("create producer worker.")
    val producerWorker: ProducerWorker[String, String] = ProducerWorker(new StringSerializer(), new StringSerializer())

    logger.info(s"add test record set. record count: ${testRecordSet10.length}")
    producerWorker.addProducerRecord(testRecordSet10)

    while (producerWorker.getBufferSize != testRecordSet10.length) {
      logger.info("wait for 1sec add test record to buffer.")
      Thread.sleep(1000)
    }

    logger.info("start mock producer worker.")
    producerWorker.start()

    logger.info("stop mock producer and wait.")
    Await.result(producerWorker.stop(), Duration.Inf)
    producerWorker.getKafkaProducer.close()
  }

  @Test
  def testThroughput(): Unit = {
    this.setRootLogLevel(Level.INFO)

    val parallelLevelList: Vector[Int] = Vector(1, 1, 1, 4, 4, 4, 8, 8, 8)
    val results: Vector[(Int, Double, Double)] = for (parallelLevel <- parallelLevelList) yield {
      throughputTask(parallelLevel)
    }

    for (result <- results) {
      println()
      println(s"==================================")
      println(s"parallel level: ${result._1}")
      println(s"throughput: ${result._2} record/sec")
      println(s"total time spend: ${result._3}")
      println(s"==================================")
    }
  }

  private def throughputTask(parallelLevel: Int): (Int, Double, Double) = {
    val produceWorker: ProducerWorker[String, String] = ProducerWorker(new StringSerializer(), new StringSerializer())

    logger.info(s"add test record to buffer. record count: ${testRecordSet1000000.length}")
    produceWorker.addProducerRecord(testRecordSet1000000)

    while (produceWorker.getBufferSize != testRecordSet1000000.length) {
      logger.info(s"wait 1sec for add test record to buffer complete. buffer size: ${produceWorker.getBufferSize}")
      Thread.sleep(1000)
    }

    logger.info("worker start.")
    val startTime: DateTime = DateTime.now()
    produceWorker.start(parallelLevel)

    while (produceWorker.getBufferSize != 0 || produceWorker.getIncompleteAsyncProduceRecordCount != 0) {}
    val timeSpend: Double = (DateTime.now().getMillis - startTime.getMillis) / 1000.0
    val throughput: Double = testRecordSet1000000.length / timeSpend

    Await.result(produceWorker.stop(), Duration.Inf)
    produceWorker.getKafkaProducer.flush()
    produceWorker.getKafkaProducer.close()

    (parallelLevel, throughput, timeSpend)
  }

  @Test
  def testThroughputMultiProducer(): Unit = {
    this.setRootLogLevel(Level.INFO)
    val producerCountList: Vector[Int] = Vector(1, 1, 1, 4, 4, 4, 8, 8, 8)

    val results: Vector[(Int, Double, Double)] = producerCountList.map(this.throughputMultiProducerTask)

    for (result <- results) {
      println()
      println(s"==================================")
      println(s"producer count: ${result._1}")
      println(s"throughput: ${BigDecimal(result._2).toString()} record/sec")
      println(s"total time spend: ${result._3}")
      println(s"==================================")
    }
  }

  def throughputMultiProducerTask(producerCount: Int): (Int, Double, Double) = {

    val producerWorkerList = for (_ <- 1 to producerCount) yield {
      ProducerWorker(new StringSerializer(), new StringSerializer())
    }

    val testRecordSet = testRecordSet1000000.length / producerCount
    val testSplitRecordSet = testRecordSet1000000.slice(0, testRecordSet)

    logger.info(s"add test record to each buffer. record count each buffer: ${testSplitRecordSet.length}")
    producerWorkerList.foreach(_.addProducerRecord(testSplitRecordSet))

    while (!producerWorkerList.forall(_.getBufferSize == testSplitRecordSet.length)) {
      logger.info(s"wait 1sec for add test record to buffer complete. buffer size: ${producerWorkerList.map(_.getBufferSize).mkString(", ")}")
      Thread.sleep(1000)
    }

    logger.info("worker start.")
    val startTime = DateTime.now()

    producerWorkerList.foreach(_.start())

    while (producerWorkerList.forall(_.getBufferSize != 0) || producerWorkerList.forall(_.getIncompleteAsyncProduceRecordCount != 0)) {}
    val timeSpend: Double = (DateTime.now().getMillis - startTime.getMillis) / 1000.0
    val throughput: Double = testRecordSet1000000.length / timeSpend

    producerWorkerList.map(_.stop()).foreach(Await.result(_, Duration.Inf))
    producerWorkerList.foreach { worker =>
      worker.getKafkaProducer.flush()
      worker.getKafkaProducer.close()
    }

    (producerCount, throughput, timeSpend)
  }

  def setRootLogLevel(level: Level): Unit = {
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(level)
  }
}
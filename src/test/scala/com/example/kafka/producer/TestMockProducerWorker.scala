package com.example.kafka.producer

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Callback, MockProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.hamcrest.CoreMatchers.is
import org.junit.{Assert, Test}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

class TestMockProducerWorker extends LazyLogging {
  private val testTopicName: String = "test"

  private val testRecordSet10: Vector[ProducerRecord[String, String]] = (1 to 10)
    .map(index => new ProducerRecord[String, String](testTopicName, s"key-$index", s"value-$index"))
    .toVector

  private val testRecordSet1000: Vector[ProducerRecord[String, String]] = (1 to 1000)
    .map(index => new ProducerRecord[String, String](testTopicName, s"key-$index", s"value-$index"))
    .toVector

  @Test
  def testMockProducer(): Unit = {
    logger.info("create mock producer.")

    val mockKafkaProducer: MockProducer[String, String] =
      new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())

    testRecordSet10.foreach(mockKafkaProducer.send)

    Thread.sleep(10)

    val producerResult = mockKafkaProducer.history()

    logger.info("producer result.\n" + producerResult.mkString("\n"))

    Assert.assertThat(testRecordSet10.length, is(producerResult.length))
  }

  @Test
  def testMockProducerWithCollback(): Unit = {
    logger.info("create mock producer.")

    val mockKafkaProducer: MockProducer[String, String] =
      new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())

    val kafkaProducerCallback = new Callback() {
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

    testRecordSet10.foreach(mockKafkaProducer.send(_, kafkaProducerCallback))

    Thread.sleep(1000)

    val producerResult = mockKafkaProducer.history()

    logger.info("producer result\n" + producerResult.take(10).mkString("\n"))

    Assert.assertThat(testRecordSet10.length, is(producerResult.length))
  }

  @Test
  def testMockProducerWorker(): Unit = {
    logger.info("create mock producer worker.")

    val mockProducerWorker: ProducerWorker[String, String] = ProducerWorker.mock(new StringSerializer(), new StringSerializer())
    val mockKafkaProducer = mockProducerWorker.getKafkaProducer.asInstanceOf[MockProducer[String, String]]

    logger.info(s"add test record set. record count: ${testRecordSet1000.length}")
    mockProducerWorker.addProducerRecord(testRecordSet1000)

    while (mockProducerWorker.getBufferSize != testRecordSet1000.length) {
      logger.info("wait for 1sec add test record to buffer.")
      Thread.sleep(1000)
    }

    logger.info("start mock producer worker.")
    mockProducerWorker.start()

    logger.info("stop mock producer and wait.")
    Await.result(mockProducerWorker.stop(), Duration.Inf)

    Assert.assertThat(mockKafkaProducer.history().size(), is(testRecordSet1000.length))
  }

  @Test
  def testMockProducerWorkerParallel(): Unit = {
    logger.info("create mock producer worker.")

    val mockProducerWorker: ProducerWorker[String, String] = ProducerWorker.mock(new StringSerializer(), new StringSerializer())
    val mockKafkaProducer: MockProducer[String, String] = mockProducerWorker.getKafkaProducer.asInstanceOf[MockProducer[String, String]]

    logger.info(s"add test record set. record count: ${testRecordSet1000.length}")
    mockProducerWorker.addProducerRecord(testRecordSet1000)

    while (mockProducerWorker.getBufferSize != testRecordSet1000.length) {
      logger.info("wait for 1sec add test record to buffer.")
      Thread.sleep(1000)
    }

    logger.info("start mock producer worker.")
    mockProducerWorker.start(4)

    logger.info("stop mock producer and wait.")
    Await.result(mockProducerWorker.stop(), Duration.Inf)

    Assert.assertThat(mockKafkaProducer.history().size(), is(testRecordSet1000.length))
  }
}

package com.example.kafka.producer

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.hamcrest.CoreMatchers.is
import org.junit.{Assert, Test}

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class TestMockProducerWorker extends LazyLogging {
  private val testTopicName: String = "test"

  private val testRecordSet10: Vector[ProducerRecord[String, String]] = (1 to 10)
    .map(index => new ProducerRecord[String, String](testTopicName, s"key-$index", s"value-$index"))
    .toVector

  private val testRecordSet1000: Vector[ProducerRecord[String, String]] = (1 to 1000)
    .map(index => new ProducerRecord[String, String](testTopicName, s"key-$index", s"value-$index"))
    .toVector

  @Test
  def testMockProducerWorker(): Unit = {
    val mockKafkaProducer: MockProducer[String, String] =
      new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())
    val emptyProperties = new Properties()

    val mockProducerClient: ProducerClient[String, String] = ProducerClient(mockKafkaProducer, emptyProperties)
    val producerWorker = ProducerWorker(mockProducerClient)

    producerWorker.start()
    producerWorker.offerProducerRecordsToBuffer(testRecordSet1000)
    Await.result(producerWorker.stop(), Duration.Inf)

    Assert.assertThat(mockKafkaProducer.history().size(), is(testRecordSet1000.size))
  }

  @Test
  def testMockProducerWithCallback(): Unit = {

  }
}

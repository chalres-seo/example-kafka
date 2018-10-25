package com.example.kafka.producer

import java.util.Collections
import java.util.concurrent.{Future => JFuture}

import com.example.kafka.admin.Admin
import com.example.kafka.consumer.KafkaConsumerFactory
import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.junit.{After, Assert, Before, Test}
import org.hamcrest.CoreMatchers._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.time.{Duration => JDuration}

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

import scala.annotation.tailrec
import scala.collection.JavaConversions._

class TestProducerWorker extends LazyLogging {
  private val testTopicName: String = "test-producer"
  private val testTopicPartitionCount: Int = 3
  private val testTopicReplicaFactor:Short = 3

  private val testTopicRecordSetCount = 10
  private val testRecordSet: Vector[ProducerRecord[String, String]] = (1 to testTopicRecordSetCount)
    .map(index => new ProducerRecord[String, String](testTopicName, s"key-$index", s"value-$index"))
    .toVector

  private val kafkaAdmin = Admin()

  @Before
  def setUp(): Unit = {
    while (kafkaAdmin.isExistTopic(testTopicName)) {
      kafkaAdmin.deleteTopic(testTopicName)
      Thread.sleep(1000)
    }

    while(!kafkaAdmin.isExistTopic(testTopicName)) {
      kafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicaFactor)
      Thread.sleep(1000)
    }
  }

  @After
  def cleanUp(): Unit = {
    while (kafkaAdmin.isExistTopic(testTopicName)) {
      kafkaAdmin.deleteTopic(testTopicName)
      Thread.sleep(1000)
    }
  }

  @Test
  def testProducer(): Unit = {
    val kafkaProducer = new KafkaProducer(AppConfig.getKafkaProducerProps, new StringSerializer(), new StringSerializer())

    val produceFutures: Vector[JFuture[RecordMetadata]] = testRecordSet.map(kafkaProducer.send)
    while(!produceFutures.forall(_.isDone)) { Thread.sleep(1000) }

    kafkaProducer.flush()

    logger.debug {
      produceFutures.map { f =>
        val metadata = f.get()
        s"succeed send record. metadata: " +
          "topic: " + metadata.topic() + ", " +
          "partition: " + metadata.partition() + ", " +
          "offset: " + metadata.offset() + ", " +
          "timestamp: " + metadata.timestamp() + ", " +
          "serialized key size: " + metadata.serializedKeySize() + ", " +
          "serialized value size: " + metadata.serializedValueSize()
      }.mkString("\n")
    }

    val records = this.consumeFromBeginning(testTopicName)
    logger.debug(records.mkString("\n"))

    Assert.assertThat(records.length, is(testTopicRecordSetCount))
    kafkaProducer.close()
  }

  @Test
  def testProducerWithCallback(): Unit = {
    val kafkaProducer = new KafkaProducer(AppConfig.getKafkaProducerProps, new StringSerializer(), new StringSerializer())

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

    val produceFutures: Vector[JFuture[RecordMetadata]] = testRecordSet.map(kafkaProducer.send(_, callback))

    while(!produceFutures.forall(_.isDone)) { Thread.sleep(1000) }

    kafkaProducer.flush()

    logger.debug {
      produceFutures.map { f =>
        val metadata = f.get()
        s"succeed send record. metadata: " +
          "topic: " + metadata.topic() + ", " +
          "partition: " + metadata.partition() + ", " +
          "offset: " + metadata.offset() + ", " +
          "timestamp: " + metadata.timestamp() + ", " +
          "serialized key size: " + metadata.serializedKeySize() + ", " +
          "serialized value size: " + metadata.serializedValueSize()
      }.mkString("\n")
    }

    val records = this.consumeFromBeginning(testTopicName)
    logger.debug(records.mkString("\n"))

    Assert.assertThat(records.length, is(testTopicRecordSetCount))
    kafkaProducer.close()
  }

  @Test
  def testProducerWorker(): Unit = {
    val producerWorker: ProducerWorker[String, String] = ProducerWorker(new StringSerializer(), new StringSerializer())

    producerWorker.start()
    producerWorker.addProduceRecords(testRecordSet)

    Await.result(producerWorker.stop(), Duration.Inf)

    val records = this.consumeFromBeginning(testTopicName)
    logger.debug(records.mkString("\n"))

    producerWorker.close()
    Assert.assertThat(records.length, is(testTopicRecordSetCount))
  }

  def consumeFromBeginning(topicName: String): Vector[ConsumerRecord[String, String]] = {
    val kafkaConsumer: KafkaConsumer[String, String] =
      KafkaConsumerFactory.createConsumer(new StringDeserializer, new StringDeserializer)

    kafkaConsumer.subscribe(Collections.singletonList(testTopicName))
    kafkaConsumer.poll(JDuration.ofMillis(1000))
    kafkaConsumer.seekToBeginning(kafkaConsumer.assignment())

    @tailrec
    def loop(records:Vector[ConsumerRecord[String, String]]):Vector[ConsumerRecord[String, String]] = {
      val consumeRecords: Vector[ConsumerRecord[String, String]] =
        kafkaConsumer.poll(JDuration.ofMillis(1000)).iterator().toVector

      if (consumeRecords.isEmpty) {
        records
      } else {
        loop(records ++ consumeRecords)
      }
    }

    val records = loop(kafkaConsumer.poll(JDuration.ofMillis(1000)).iterator().toVector)

    kafkaConsumer.close()
    records
  }
}
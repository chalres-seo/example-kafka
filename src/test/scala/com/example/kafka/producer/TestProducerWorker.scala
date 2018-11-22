package com.example.kafka.producer

import java.time.{Duration => JDuration}
import java.util.Collections

import com.example.kafka.admin.AdminClient
import com.example.kafka.consumer.ConsumerClient
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit._
import org.hamcrest.CoreMatchers._
import com.example.kafka.producer.TestProducerWorker._
import com.example.utils.AppConfig
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import scala.collection.JavaConversions._

class TestProducerWorker {

  @Test
  def testProduceRecords(): Unit = {
    val producerWorker = ProducerWorker[Any, Any](AppConfig.createDefaultKafkaProducerProps, true)

    producerWorker.start()
    producerWorker.offerProducerRecordsToBuffer(testProduceRecordSet)

    Await.result(producerWorker.stop(), Duration.Inf)
    producerWorker.close()

    val result = this.consumeFromBeginning(testTopicName)

    Assert.assertThat(result.length, is(testProduceRecordSetCount))

  }

  def consumeFromBeginning(topicName: String): Vector[ConsumerRecord[String, String]] = {
    val kafkaConsumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](AppConfig.createDefaultKafkaConsumerProps)

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

object TestProducerWorker {
  private val testTopicName = "test-kafka-producer-worker"
  private val testTopicPartitionCount = 3
  private val testTopicReplicationFactor: Short = 3

  private val testProduceRecordSetCount = 100
  private  val testProduceRecordSet: Vector[ProducerRecord[Any, Any]] =
    (1 to testProduceRecordSetCount).map { i =>
      new ProducerRecord(testTopicName, s"key-$i".asInstanceOf[Any], s"value-$i".asInstanceOf[Any])
    }.toVector

  private val testAdminClient = AdminClient(AppConfig.createDefaultKafkaAdminProps)

  @BeforeClass
  def beforeClass(): Unit = {
    this.createTestTopic()
  }

  @AfterClass
  def tearDownClass(): Unit = {

    this.deleteTestTopic()
    testAdminClient.close()
  }

  def createTestTopic(): Unit = {
    if (!testAdminClient.isExistTopic(testTopicName)) {
      testAdminClient.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
      while(!testAdminClient.isExistTopic(testTopicName)) {
        testAdminClient.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
        Thread.sleep(500)
      }
    }
  }

  def deleteTestTopic(): Unit = {
    if (testAdminClient.isExistTopic(testTopicName)) {
      testAdminClient.deleteTopic(testTopicName).get
      while(testAdminClient.isExistTopic(testTopicName)) {
        testAdminClient.deleteTopic(testTopicName).get
        Thread.sleep(500)
      }
    }
  }
}
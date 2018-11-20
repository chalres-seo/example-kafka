package com.example.kafka.consumer

import java.util

import com.example.kafka.admin.AdminClient
import com.example.kafka.producer.ProducerClient
import com.example.utils.AppConfig
import org.apache.kafka.clients.producer.ProducerRecord

import org.junit.{AfterClass, Assert, BeforeClass, Test}
import org.hamcrest.CoreMatchers._

import com.example.kafka.consumer.TestConsumerClient._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}

import scala.annotation.tailrec
import scala.collection.JavaConversions._

class TestConsumerClient {

  @Test
  def testConsumeRecords(): Unit = {
    testConsumerClient.subscribeTopic(testTopicName)

    @tailrec
    def loop(consumerRecords: ConsumerRecords[Any, Any], consumedRecordList: util.Iterator[ConsumerRecord[Any, Any]]): util.Iterator[ConsumerRecord[Any, Any]] = {
      if (consumerRecords.isEmpty) {
        testConsumerClient.offsetCommit()
        consumedRecordList
      } else {
        testConsumerClient.offsetCommitAsync()
        loop(testConsumerClient.consumeRecord, consumedRecordList ++ consumerRecords.iterator())
      }
    }

    val consumeRecordForPrepare: util.Iterator[ConsumerRecord[Any, Any]] = loop(testConsumerClient.consumeRecord, Iterator.empty)
    Assert.assertThat(consumeRecordForPrepare.length, is(0))

    TestConsumerClient.produceTestRecordSet()

    val consumeRecord: util.Iterator[ConsumerRecord[Any, Any]] = loop(testConsumerClient.consumeRecord, Iterator.empty)
    Assert.assertThat(consumeRecord.length, is(testProduceRecordSetCount))
  }
}

object TestConsumerClient {
  val testTopicName = "test-kafka-consumer"
  val testTopicPartitionCount = 3
  val testTopicReplicationFactor: Short = 3

  val testProduceRecordSetCount = 100
  val testProduceRecordSet: Vector[ProducerRecord[Any, Any]] =
    (1 to testProduceRecordSetCount).map { i =>
      new ProducerRecord(testTopicName, s"key-$i".asInstanceOf[Any], s"value-$i".asInstanceOf[Any])
    }.toVector

  val testKafkaAdmin = AdminClient(AppConfig.createDefaultKafkaAdminProps)
  val testProducerClient: ProducerClient[Any, Any] = ProducerClient[Any, Any](AppConfig.createDefaultKafkaProducerProps)
  var testConsumerClient: ConsumerClient[Any, Any] = _

  def produceTestRecordSet(): Unit = {
    testProducerClient
      .sendProducerRecords(testProduceRecordSet)
      .foreach(_.get)
    Thread.sleep(3000)
  }

  @BeforeClass
  def beforeClass(): Unit = {
    testConsumerClient = ConsumerClient(AppConfig.createDefaultKafkaConsumerProps)

    this.deleteTestTopic
    this.createTestTopic
  }

  @AfterClass
  def tearDownClass(): Unit = {
    testConsumerClient.close()

    this.deleteTestTopic

    testKafkaAdmin.close()
    testProducerClient.close()
  }

  def createTestTopic = {
    if (!testKafkaAdmin.isExistTopic(testTopicName)) {
      testKafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get

      while(!testKafkaAdmin.isExistTopic(testTopicName)) {
        testKafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
        Thread.sleep(500)
      }
    }
  }

  def deleteTestTopic = {
    if (testKafkaAdmin.isExistTopic(testTopicName)) {
      testKafkaAdmin.deleteTopic(testTopicName).get

      while(testKafkaAdmin.isExistTopic(testTopicName)) {
        testKafkaAdmin.deleteTopic(testTopicName).get
        Thread.sleep(500)
      }
    }
  }

  def closeResource = {
    testKafkaAdmin.close()
    testProducerClient.close()
    testConsumerClient.close()
  }
}
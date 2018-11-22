package com.example.kafka.consumer

import com.example.kafka.admin.AdminClient
import com.example.kafka.producer.ProducerClient
import com.example.utils.AppConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.{AfterClass, Assert, BeforeClass, Test}
import org.hamcrest.CoreMatchers._

import scala.collection.JavaConversions._
import com.example.kafka.consumer.TestConsumerWorker._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class TestConsumerWorker {
  @Test
  def testConsumeRecords(): Unit = {
    val consumerWorker = ConsumerWorker(AppConfig.createDefaultKafkaConsumerProps, testTopicName)

    consumerWorker.start()
    Thread.sleep(3000L)

    TestConsumerWorker.produceTestRecordSet()
    Thread.sleep(3000L)

    val consumerStopFuture = consumerWorker.stop()
    Thread.sleep(3000L)

    val consumeRecords = consumerWorker.getAllConsumerRecordsFromBuffer

    Await.result(consumerStopFuture, Duration.Inf)
    consumerWorker.close()

    Assert.assertThat(consumeRecords.get.flatMap(_.iterator()).length, is(testProduceRecordSetCount))

  }
}

object TestConsumerWorker {
  private val testTopicName = "test-kafka-consumer-worker"
  private val testTopicPartitionCount = 3
  private val testTopicReplicationFactor: Short = 3

  private val testProduceRecordSetCount = 100
  private val testProduceRecordSet: Vector[ProducerRecord[Any, Any]] =
    (1 to testProduceRecordSetCount).map { i =>
      new ProducerRecord(testTopicName, s"key-$i".asInstanceOf[Any], s"value-$i".asInstanceOf[Any])
    }.toVector

  private val testKafkaAdmin = AdminClient(AppConfig.createDefaultKafkaAdminProps)

  def produceTestRecordSet(): Unit = {
    val testProducerClient: ProducerClient[Any, Any] = ProducerClient[Any, Any](AppConfig.createDefaultKafkaProducerProps)

    testProducerClient
      .sendProducerRecords(testProduceRecordSet)
      .foreach(_.get)
    Thread.sleep(3000)

    testProducerClient.close()
  }

  @BeforeClass
  def beforeClass(): Unit = {
    this.createTestTopic()
  }

  @AfterClass
  def tearDownClass(): Unit = {
    this.deleteTestTopic()
    testKafkaAdmin.close()
  }

  def createTestTopic(): Unit = {
    if (!testKafkaAdmin.isExistTopic(testTopicName)) {
      testKafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get

      while(!testKafkaAdmin.isExistTopic(testTopicName)) {
        Thread.sleep(500)
        testKafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicationFactor).get
      }
    }
  }

  def deleteTestTopic(): Unit = {
    if (testKafkaAdmin.isExistTopic(testTopicName)) {
      testKafkaAdmin.deleteTopic(testTopicName).get

      while(testKafkaAdmin.isExistTopic(testTopicName)) {
        Thread.sleep(500)
        testKafkaAdmin.deleteTopic(testTopicName).get
      }
    }
  }

}
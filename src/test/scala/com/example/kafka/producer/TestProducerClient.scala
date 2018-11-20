package com.example.kafka.producer

import com.example.kafka.admin.AdminClient
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.junit._
import org.hamcrest.CoreMatchers._
import com.example.kafka.producer.TestProducerClient._
import com.example.utils.AppConfig

class TestProducerClient {

  @Test
  def testProduceRecords(): Unit = {
    val result: Vector[RecordMetadata] = testProducerClient.sendProducerRecords(testProduceRecordSet).map(_.get)
    val resultMetaData: Vector[Map[String, String]] = result.map(ProducerClient.produceRecordMetadataToMap)

    Assert.assertThat(resultMetaData.length, is(testProduceRecordSetCount))
    Assert.assertThat(resultMetaData.count(_("topic").toString == testTopicName), is(testProduceRecordSetCount))
  }
}

object TestProducerClient {
  val testTopicName = "test-kafka-producer"
  val testTopicPartitionCount = 3
  val testTopicReplicationFactor: Short = 3

  val testProduceRecordSetCount = 100
  val testProduceRecordSet: Vector[ProducerRecord[Any, Any]] =
    (1 to testProduceRecordSetCount).map { i =>
      new ProducerRecord(testTopicName, s"key-$i".asInstanceOf[Any], s"value-$i".asInstanceOf[Any])
    }.toVector

  val testAdminClient = AdminClient(AppConfig.createDefaultKafkaAdminProps)
  var testProducerClient: ProducerClient[Any, Any] = ProducerClient[Any, Any](AppConfig.createDefaultKafkaProducerProps)

  @BeforeClass
  def beforeClass(): Unit = {
    this.createTestTopic()
  }

  @AfterClass
  def tearDownClass(): Unit = {
    this.deleteTestTopic()

    testProducerClient.close()
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
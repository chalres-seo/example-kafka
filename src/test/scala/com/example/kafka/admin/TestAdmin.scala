package com.example.kafka.admin

import java.util
import java.util.Scanner

import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, KafkaAdminClient, TopicDescription}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.errors.TopicExistsException
import org.junit._
import org.hamcrest.CoreMatchers._
import org.junit.runners.MethodSorters

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class TestAdmin extends LazyLogging {
  private val testTopicName = "test-admin"
  private val testTopicPartitionCount = 3
  private val testTopicReplicaFactor: Short = 3

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
  def testCreateAndDeleteTopic(): Unit = {
    val _testTopicName = "teexit" +
      "st-create-and-delete-topic"

    while (kafkaAdmin.isExistTopic(_testTopicName)) {
      kafkaAdmin.deleteTopic(_testTopicName).get
      Thread.sleep(1000)
    }

    kafkaAdmin.createTopic(_testTopicName, testTopicPartitionCount, testTopicReplicaFactor)
    while (!kafkaAdmin.isExistTopic(_testTopicName)) {
      kafkaAdmin.createTopic(_testTopicName, testTopicPartitionCount, testTopicReplicaFactor)
      Thread.sleep(1000)
    }

    Assert.assertThat(kafkaAdmin.getTopicNameList.get.contains(_testTopicName), is(true))
    Assert.assertTrue(kafkaAdmin.isExistTopic(_testTopicName))

    kafkaAdmin.createTopic(_testTopicName, testTopicPartitionCount, testTopicReplicaFactor)

    kafkaAdmin.deleteTopic(_testTopicName).get
    while (kafkaAdmin.isExistTopic(_testTopicName)) {
      kafkaAdmin.deleteTopic(_testTopicName).get
      Thread.sleep(1000)
    }

    Assert.assertThat(kafkaAdmin.getTopicNameList.get.contains(_testTopicName), is(false))
    Assert.assertFalse(kafkaAdmin.isExistTopic(_testTopicName))
  }

  @Test
  def testDescribeAndPartitionTopic(): Unit = {
    val topicDescription = kafkaAdmin.describeTopic(testTopicName).get().get(testTopicName)
    val topicPartitionInfoList = kafkaAdmin.getTopicPartitionInfo(testTopicName).get()
    val topicPartitionCount = kafkaAdmin.getTopicPartitionCount(testTopicName).get()

    Assert.assertThat(topicDescription.name(), is(testTopicName))
    Assert.assertFalse(topicDescription.isInternal)
    Assert.assertThat(topicPartitionInfoList.size(), is(testTopicPartitionCount))
    Assert.assertThat(topicPartitionCount, is(testTopicPartitionCount))
    Assert.assertTrue(topicPartitionInfoList.map(_.replicas().size).forall(_ == testTopicReplicaFactor))
  }

  @Test
  def testDeleteAllTopic(): Unit = {
    val topicList = Vector("test1-topic", "test2-topic", "test3-topic")
    topicList.foreach(kafkaAdmin.createTopic(_, testTopicPartitionCount, testTopicReplicaFactor))

    val kafkaTopicNameList = kafkaAdmin.getTopicNameList.get

    Assert.assertTrue(topicList.forall(kafkaTopicNameList.contains(_)))

    kafkaAdmin.deleteAllTopics().get

    Assert.assertThat(kafkaAdmin.getTopicNameList.get.size(), is(0))
  }
}

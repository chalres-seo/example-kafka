package com.example.kafka.consumer

import java.lang
import java.time.{Duration => JDuration}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.hamcrest.CoreMatchers.is
import org.junit.{Assert, Test}

import scala.collection.JavaConversions._

class TestMockConsumerWorker extends LazyLogging {
  private val testTopicName: String = "test"
  private val testTopicPartitionCount = 3
  private val testRecordCount = 10

  private val testRecordSet: Vector[ConsumerRecord[String, String]] = (1 to testRecordCount)
    .map(index => new ConsumerRecord[String, String](testTopicName,
      index % testTopicPartitionCount,
      index - 1,
      s"key-$index",
      s"value-$index"))
    .toVector


  @Test
  def testMockConsumer(): Unit = {

    val mockConsumer = this.setupMockConsumer

    val consumeRecords = mockConsumer.poll(JDuration.ofMillis(3000))

    Assert.assertThat(consumeRecords.count(), is(testRecordCount))
  }


  def setupMockConsumer: MockConsumer[String, String] = {
    val mockKafkaConsumer: MockConsumer[String, String] = new MockConsumer[String, String](OffsetResetStrategy.LATEST)
    val mockPartitions: Vector[TopicPartition] = this.createMockTopicPartition(testTopicName, testTopicPartitionCount)

    mockKafkaConsumer.subscribe(Vector(testTopicName))
    mockKafkaConsumer.rebalance(mockPartitions)
    mockKafkaConsumer.updateBeginningOffsets(this.createMockPartitionsOffsets(mockPartitions, 0))

    testRecordSet.foreach(mockKafkaConsumer.addRecord)
    mockKafkaConsumer.addEndOffsets(this.createMockPartitionsOffsets(mockPartitions, 0))

    mockKafkaConsumer.subscribe(Vector(testTopicName))
    mockKafkaConsumer
  }

  def createMockTopicPartition(topicName: String, partitionCount: Int): Vector[TopicPartition] = {
    (0 until partitionCount).map(new TopicPartition(topicName, _)).toVector
  }

  def createMockPartitionsOffsets(topicPartitions: Vector[TopicPartition], offset: Long): Map[TopicPartition, lang.Long] = {
    topicPartitions.map(p => p -> long2Long(offset)).toMap
  }
}

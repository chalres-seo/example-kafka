package com.example.kafka.consumer

import java.lang
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.hamcrest.CoreMatchers._
import org.junit._

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class TestMockConsumerWorker {
  val testTopicName = "test-mock-kafka-consumer-worker"
  val testTopicPartitionCount = 3
  val testTopicReplicationFactor: Short = 3

  val testConsumerRecordSetCount = 100
  val testConsumerRecordSet: Vector[ConsumerRecord[String, String]] =
    (1 to testConsumerRecordSetCount).map { i =>
      new ConsumerRecord(testTopicName, i % testTopicPartitionCount, i -1, s"key-$i", s"value-$i")
    }.toVector

  val mockKafkaConsumer: MockConsumer[String, String] = this.createPreparedMockConsumer
  val mockConsumerClient: ConsumerClient[String, String] = ConsumerClient(mockKafkaConsumer, new Properties())
  val mockConsumerWorker = ConsumerWorker(mockConsumerClient, testTopicName)

  @Test
  def testConsumeRecord(): Unit = {
    mockConsumerWorker.start()
    Thread.sleep(3000L)
    val consumerStopFuture = mockConsumerWorker.stop()

    val result = mockConsumerWorker.getAllConsumerRecordsFromBuffer

    Await.result(consumerStopFuture, Duration.Inf)

    Assert.assertThat(result.get.flatMap(_.iterator()).length, is(testConsumerRecordSetCount))
  }

  def createPreparedMockConsumer: MockConsumer[String, String] = {
    val mockKafkaConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)

    val mockPartitions: Vector[TopicPartition] = (0 until testTopicPartitionCount).map(new TopicPartition(testTopicName, _)).toVector
    val mockPartitionsOffsets: Map[TopicPartition, lang.Long] = mockPartitions.map(p => p -> long2Long(0L)).toMap

    mockKafkaConsumer.subscribe(Collections.singleton(testTopicName))
    mockKafkaConsumer.rebalance(mockPartitions)
    mockKafkaConsumer.updateBeginningOffsets(mockPartitionsOffsets)
    testConsumerRecordSet.foreach(mockKafkaConsumer.addRecord)
    mockKafkaConsumer.addEndOffsets(mockPartitionsOffsets)

    mockKafkaConsumer
  }
}

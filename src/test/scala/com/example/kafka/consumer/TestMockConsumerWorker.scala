package com.example.kafka.consumer

import java.lang
import java.time.{Duration => JDuration}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.hamcrest.CoreMatchers.is
import org.junit.{Assert, Test}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

// TODO: test mock consumer
class TestMockConsumerWorker extends LazyLogging {
  private val testTopicName: String = "test"
  private val testTopicPartitionCount = 1
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

//    while(true) {
//      mockConsumer.poll(JDuration.ofMillis(3000)).foreach(println)
//    }

    mockConsumer.poll(JDuration.ofMillis(3000)).foreach(println)
    mockConsumer.poll(JDuration.ofMillis(3000)).foreach(println)
    //    val mockKafkaConsumer = this.setupMockConsumer
//    val consumeRecords = mockKafkaConsumer.poll(JDuration.ofMillis(1000))
//
//    logger.info("consumer result.\n" + consumeRecords.records(testTopicName).mkString("\n"))
//
//    println("wait")
//    val consumeRecords2 = mockKafkaConsumer.poll(JDuration.ofMillis(10000))
//    logger.info("consumer2 result.\n" + consumeRecords2.records(testTopicName).mkString("\n"))
//
//    Assert.assertThat(consumeRecords.count(), is(testRecordCount))
//    mockKafkaConsumer.close()
  }

  @Test
  def testMockConsumerWorker(): Unit = {
    val mockConsumerWorker: ConsumerWorker[String, String] = this.setupMockConsumerWorker
    val mockKafkaConsumer = mockConsumerWorker.getKafkaConsumer.asInstanceOf[MockConsumer[String, String]]

    mockConsumerWorker.start()

    println(mockConsumerWorker.getBufferSize)
    Thread.sleep(1000)
    println(mockConsumerWorker.getBufferSize)
    Thread.sleep(1000)
    println(mockConsumerWorker.getBufferSize)
    Thread.sleep(1000)
    println(mockConsumerWorker.getBufferSize)
    Thread.sleep(1000)
    println(mockConsumerWorker.getBufferSize)
    Thread.sleep(1000)

    Await.result(mockConsumerWorker.stop(), Duration.Inf)
    mockConsumerWorker.getKafkaConsumer.close()
  }

  def setupMockConsumerWorker: ConsumerWorker[String, String] = {
    val mockConsumerWorker = ConsumerWorker.mock[String, String](OffsetResetStrategy.LATEST)
    val mockKafkaConsumer = mockConsumerWorker.getKafkaConsumer.asInstanceOf[MockConsumer[String, String]]
    val mockPartitions: Vector[TopicPartition] = this.createMockTopicPartition(testTopicName, testTopicPartitionCount)

    mockKafkaConsumer.subscribe(Vector(testTopicName))
    mockKafkaConsumer.rebalance(mockPartitions)
    mockKafkaConsumer.updateBeginningOffsets(this.createMockPartitionsOffsets(mockPartitions, 0))

    testRecordSet.foreach(mockKafkaConsumer.addRecord)
    mockKafkaConsumer.addEndOffsets(this.createMockPartitionsOffsets(mockPartitions, 0))

    mockConsumerWorker
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

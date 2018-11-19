//package com.example.kafka.consumer
//
//import java.lang
//import java.util.{Collections, Properties}
//import java.util
//
//import com.example.kafka.producer.ProducerClient
//import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, MockConsumer, OffsetResetStrategy}
//import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}
//import org.apache.kafka.common.TopicPartition
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.hamcrest.CoreMatchers._
//import org.junit._
//
//import scala.annotation.tailrec
//import scala.collection.JavaConverters._
//import scala.collection.JavaConversions._
//
//class TestMockConsumerClient {
//  val testTopicName = "test-mock-kafka-consumer"
//  val testTopicPartitionCount = 3
//  val testTopicReplicationFactor: Short = 3
//
//  val testConsumerRecordSetCount = 100
//  val testConsumerRecordSet: Vector[ConsumerRecord[String, String]] =
//    (1 to testConsumerRecordSetCount).map { i =>
//      new ConsumerRecord(testTopicName, i % testTopicPartitionCount, i -1, s"key-$i", s"value-$i")
//    }.toVector
//
//  val mockKafkaConsumer: MockConsumer[String, String] = this.createPreparedMockConsumer
//  val mockConsumerClient = ConsumerClient(mockKafkaConsumer, new Properties())
//
//  @Test
//  def testConsumeRecord(): Unit = {
//    @tailrec
//    def loop(consumerRecords: ConsumerRecords[String, String], result: util.Iterator[ConsumerRecord[String, String]]): util.Iterator[ConsumerRecord[String, String]] = {
//      if (consumerRecords.isEmpty) {
//        mockConsumerClient.offsetCommit()
//        result
//      } else {
//        val nextResult = mockConsumerClient.consumeRecord
//        mockConsumerClient.offsetCommitAsync()
//        loop(nextResult, result ++ nextResult.iterator())
//      }
//    }
//
//    val result: ConsumerRecords[String, String] = mockConsumerClient.consumeRecord
//    val consumeRecord: util.Iterator[ConsumerRecord[String, String]] = loop(result, result.iterator())
//
//    Assert.assertThat(consumeRecord.length, is(testConsumerRecordSetCount))
//  }
//
//  def createPreparedMockConsumer: MockConsumer[String, String] = {
//    val mockKafkaConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
//
//    val mockPartitions: Vector[TopicPartition] = (0 until testTopicPartitionCount).map(new TopicPartition(testTopicName, _)).toVector
//    val mockPartitionsOffsets: Map[TopicPartition, lang.Long] = mockPartitions.map(p => p -> long2Long(0L)).toMap
//
//    mockKafkaConsumer.subscribe(Collections.singleton(testTopicName))
//    mockKafkaConsumer.rebalance(mockPartitions)
//    mockKafkaConsumer.updateBeginningOffsets(mockPartitionsOffsets)
//    testConsumerRecordSet.foreach(mockKafkaConsumer.addRecord)
//    mockKafkaConsumer.addEndOffsets(mockPartitionsOffsets)
//
//    mockKafkaConsumer
//  }
//}

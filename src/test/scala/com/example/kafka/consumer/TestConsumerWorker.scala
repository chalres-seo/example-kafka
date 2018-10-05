package com.example.kafka.consumer

import java.{lang, util}
import java.time.Duration
import java.util.Collections
import java.util.regex.Pattern

import com.example.kafka.producer.KafkaProducerFactory
import com.typesafe.scalalogging.LazyLogging
import kafka.cluster.Partition
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.{PartitionInfo, TopicPartition, TopicPartitionInfo}
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.{Assert, Test}
import org.hamcrest.CoreMatchers._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

// TODO: test consumer worker
class TestConsumerWorker extends LazyLogging {
  @Test
  def testConsumerWorker(): Unit = {
    val testTopicName: String = "test-consume"
    val consumer = KafkaConsumerFactory.createConsumer(new StringDeserializer, new StringDeserializer)
    consumer.subscribe(Vector(testTopicName))

//    while(true) {
//      consumer.poll(Duration.ofMillis(Long.MaxValue)).foreach(println)
//    }

    consumer.poll(Duration.ofMillis(3000)).foreach(println)
    consumer.poll(Duration.ofMillis(3000)).foreach(println)
  }
}

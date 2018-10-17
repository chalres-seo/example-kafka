package com.example.kafka.admin

import java.util

import com.typesafe.scalalogging.LazyLogging
import kafka.common.KafkaException
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.{KafkaFuture, TopicPartition, TopicPartitionInfo}

import scala.util.Try


class Admin(kafkaAdminClient: KafkaAdminClient) extends LazyLogging {

  def createTopic(name: String, numPartitions: Int, replicationFactor: Short): Unit = {
    val newTopic = new NewTopic(name, numPartitions, replicationFactor)
    logger.debug(s"create topic. info: ${newTopic.toString}")

    try {
      kafkaAdminClient.createTopics(util.Collections.singleton(newTopic)).all().get
    } catch {
      //case e:org.apache.kafka.common.errors.TopicExistsException => logger.warn(s"failed create topic. topic: $name", e)
      //case e:java.util.concurrent.ExecutionException => logger.warn(s"failed create topic. topic: $name", e)
      case e:java.util.concurrent.ExecutionException => e.getCause match {
        case _:org.apache.kafka.common.errors.TopicExistsException => logger.warn(s"failed create topic, topic exist. topic: $name")
        case t => logger.error(s"failed create topic. name: $name", t)
      }
      case e:Exception => logger.error(s"failed create topic. name: $name", e)
    }
  }

  def getTopicNameList: KafkaFuture[util.Set[String]] = {
    logger.debug("get topic name list.")
    kafkaAdminClient.listTopics().names()
  }

  def isExistTopic(name: String): Boolean = {
    this.getTopicNameList.get.contains(name)
  }

  def deleteTopic(name: String): KafkaFuture[Void] = {
    logger.debug(s"delete topic. name: $name")
    kafkaAdminClient.deleteTopics(util.Collections.singleton(name)).all()
  }


  def deleteAllTopics(): KafkaFuture[Void] = {
    logger.debug("delete all topics.")
    this.getTopicNameList.thenApply(new KafkaFuture.BaseFunction[util.Set[String], KafkaFuture[Void]] {
      override def apply(topicNameList: util.Set[String]): KafkaFuture[Void] = {
        kafkaAdminClient.deleteTopics(topicNameList).all()
      }
    }).get
  }

  def getConsumerGroupIdList: KafkaFuture[util.Collection[ConsumerGroupListing]] = {
    logger.debug("get consumer group id list.")
    kafkaAdminClient.listConsumerGroups().all()
  }

  def getConsumerGroupOffset(groupId: String): KafkaFuture[util.Map[TopicPartition, OffsetAndMetadata]] = {
    logger.debug(s"get consumer group offsets. group id: $groupId")
    kafkaAdminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
  }

  def describeTopic(name: String): KafkaFuture[util.Map[String, TopicDescription]] = {
    logger.debug(s"get describe topic. name: $name")
    kafkaAdminClient.describeTopics(util.Collections.singletonList(name)).all()
  }

  private def kafkaFutureApply[I, O](fn: I => O): KafkaFuture.BaseFunction[I, O] = {
    new KafkaFuture.BaseFunction[I, O] { override def apply(input: I): O = fn(input) }
  }

  def getTopicPartitionInfo(name: String): KafkaFuture[util.List[TopicPartitionInfo]] = {
    logger.debug(s"get topic partition info. name: $name")
    this.describeTopic(name)
      .thenApply(this.kafkaFutureApply[util.Map[String, TopicDescription], util.List[TopicPartitionInfo]] { topicDescriptions =>
        topicDescriptions.get(name).partitions()
      })
  }

  def getTopicPartitionCount(name: String): KafkaFuture[Int] = {
    this.getTopicPartitionInfo(name)
      .thenApply(this.kafkaFutureApply[util.List[TopicPartitionInfo], Int]{ topicPartitionsInfo =>
        topicPartitionsInfo.size()
      })
  }

  def close(): Unit = kafkaAdminClient.close()
}


object Admin {
  private val kafkaAdmin = createKafkaAdminClient

  def apply(): Admin = kafkaAdmin

  private def createKafkaAdminClient = new Admin(KafkaAdminFactory.createAdminClient())
}
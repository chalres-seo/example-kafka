package com.example.kafka.admin

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import com.example.utils.AppConfig
import com.example.utils.AppConfig.KafkaClientType
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.{KafkaFuture, TopicPartitionInfo}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * kafka admin client implements class.
  *
  * @see [[KafkaAdminClient]]
  * @see [[AdminClient]]
  *
  * @param kafkaAdminClient kafka admin client.
  * @param props kafka admin properties.
  */
class AdminClient(kafkaAdminClient: KafkaAdminClient, props: Properties) extends LazyLogging {
  private val dummyKafkaFuture: KafkaFuture[Void] = KafkaFutureVariation.dummyKafkaCompletedFuture

  def getClientId: String = props.getProperty("client.id")

  @throws[Exception]
  def describeTopic(name: String): KafkaFuture[util.Map[String, TopicDescription]] = {
    logger.debug(s"get describe topic. name: $name")
    kafkaAdminClient.describeTopics(util.Collections.singletonList(name)).all
  }

  @throws[Exception]
  def describeTopics(nameList: Vector[String]): util.Map[String, KafkaFuture[TopicDescription]] = {
    logger.debug("get describe topics. name list: " + nameList.mkString(", "))

    kafkaAdminClient.describeTopics(nameList.asJava).values()
  }

  @throws[Exception]
  def getTopicNameList: KafkaFuture[util.Set[String]] = {
    logger.debug("get topic name list.")
    kafkaAdminClient.listTopics().names()
  }

  def isExistTopic(name: String): Boolean = {
    logger.debug(s"check topic exist. name: $name")

    try {
      this.describeTopic(name).get.get(name).name() == name
    } catch {
      case _:Throwable => false
    }
  }

  @throws[Exception]
  def createTopic(name: String, partitionCount: Int, replicationFactor: Short): KafkaFuture[Void] = {
    logger.info(s"create topic. name: $name, partition count: $partitionCount, replication factor: $replicationFactor")
    if (this.isExistTopic(name)) {
      logger.info(s"create topic is already exist. name: $name")
      dummyKafkaFuture
    } else {
      KafkaFutureVariation.whenComplete {
        kafkaAdminClient
          .createTopics(util.Collections.singleton(new NewTopic(name, partitionCount, replicationFactor)))
          .all()
      } { (_, exception) =>
        if (exception == null) {
          logger.info(s"succeed create topic. name: $name")
        } else {
          logger.error(s"failed create topic. name: $name", exception)
          throw exception
        }
      }
    }

  }

  @throws[Exception]
  def deleteTopic(name: String): KafkaFuture[Void] = {
    logger.info(s"delete topic. name: $name")
    if (this.isExistTopic(name)) {
      KafkaFutureVariation.whenComplete {
        kafkaAdminClient
          .deleteTopics(util.Collections.singleton(name))
          .all()
      } { (_, exception) =>
        if (exception == null) {
          logger.info(s"succeed delete topic. name: $name")
        } else {
          logger.error(s"failed delete topic. name: $name", exception)
          throw exception
        }
      }
    } else {
      logger.info(s"delete topic is not exist. name: $name")
      dummyKafkaFuture
    }
  }

  @throws[Exception]
  def getTopicPartitionInfo(topicName: String): KafkaFuture[util.List[TopicPartitionInfo]] = {
    logger.debug("get topic partition info.")
    KafkaFutureVariation.thenApply {
      this.describeTopic(topicName)
    } {
      describeTopicResult => describeTopicResult(topicName).partitions()
    }
  }

  @throws[Exception]
  def close(): Unit = {
    logger.info("close admin kafka client.")
    kafkaAdminClient.close()
  }
}

object AdminClient extends LazyLogging {
  private val defaultKafkaAdminClientPrefix = AppConfig.getKafkaClientPrefix(KafkaClientType.admin)
  private val kafkaAdminClientIdNum = new AtomicInteger(1)

  /** constructor */
  def apply(kafkaAdminClient: KafkaAdminClient, props: Properties): AdminClient = {
    logger.info("create [AdminClient].")
    new AdminClient(kafkaAdminClient, props)
  }

  /** constructor overload */
  def apply(props: Properties, clientId: String): AdminClient = {
    val copyProps = AppConfig.copyProperties(props)
    copyProps.setProperty("client.id", clientId)

    this.apply(this.createKafkaAdminClient(copyProps), copyProps)
  }

  /** constructor overload */
  def apply(props: Properties): AdminClient = {
    this.apply(props, this.getAutoIncrementClientId(props))
  }

  /** kafka admin client factory api */
  private def createKafkaAdminClient(props: Properties): KafkaAdminClient = {
    logger.info(s"create [KafkaAdminClient]. client.id: ${props.getProperty("client.id")}")
    logger.info("[KafkaAdminClient] configs:\n\t" + props.mkString("\n\t"))
    org.apache.kafka.clients.admin.AdminClient.create(props).asInstanceOf[KafkaAdminClient]
  }

  private def getAutoIncrementClientId(props: Properties): String = {
    props.getOrDefault("client.id", defaultKafkaAdminClientPrefix) + "-" + kafkaAdminClientIdNum.getAndIncrement()
  }
}
package com.example.kafka.consumer

import java.time.Duration
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.atomic.AtomicInteger

import com.example.kafka.metrics.KafkaMetrics
import com.example.utils.AppConfig
import com.example.utils.AppConfig.KafkaClientType
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

/** Kafka consumer client implements class
  *
  * @see [[Consumer]]
  * @see [[KafkaConsumer]]
  * @see [[MockConsumer]]
  *
  * @param kafkaConsumer kafka consumer client
  * @param props kafka consumer properties
  * @tparam K consumer record key serializer
  * @tparam V consumer record value serializer
  */
class ConsumerClient[K, V](kafkaConsumer: Consumer[K, V], props: Properties) extends LazyLogging {
  private val defaultConsumeRecordOffsetCommitCallback = ConsumerClient.createDefaultConsumeRecordOffsetCommitCallBack
  private val groupId = props.getProperty("group.id")
  private val clientId = props.getProperty("client.id")
  private val consumerMetrics: KafkaMetrics = KafkaMetrics(kafkaConsumer.metrics())
  private val pollingWaitTimeMillis = props.getProperty("fetch.max.wait.ms", "500").toLong * 3L

  def getGroupId: String = groupId
  def getClientId: String = clientId
  def getProps: Properties = this.props
  def getMetrics: KafkaMetrics = this.consumerMetrics

  @throws[Exception]
  def subscribeTopic(name: String): Unit = {
    logger.info(s"subscribe topic. name: $name")

    kafkaConsumer.subscribe(Collections.singleton(name))
  }

  @throws[Exception]
  def unsubscribeAllTopic(): Unit = {
    logger.info("unsubscribe all topics.")

    kafkaConsumer.unsubscribe()
  }

  @throws[Exception]
  def getSubscribeTopicList: util.Set[String] = {
    logger.info("get subscribe topic list.")

    kafkaConsumer.subscription()
  }

  def getAssignmentTopicPartitionInfo: util.Set[TopicPartition] = {
    logger.info("get assignment partition count.")

    kafkaConsumer.assignment()
  }

  @throws[Exception]
  def consumeRecord: ConsumerRecords[K, V] = {
    logger.info(s"consume record. (wait time for polling is $pollingWaitTimeMillis mills.")

    val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(pollingWaitTimeMillis))

    logger.info(s"consumed record count: ${consumerRecords.count()}")

    if (consumerRecords.count() > 0) {
      logger.info("consumed record metadata:\n\t" + s"${
        consumerRecords.partitions().map { p =>
          val consumerRecordForPartition = consumerRecords.records(p)
          s"partition: $p, consume record count: ${consumerRecordForPartition.size}, last offset: ${consumerRecordForPartition.last.offset()}"
        }.mkString("\n\t")
      }")
    } else {
      logger.info("consumed record metadata: no consumed record.")
    }

    consumerRecords
  }

  @throws[Exception]
  def offsetCommit(): Unit = {
    logger.info("commit offset.")

    kafkaConsumer.commitSync()
  }

  @throws[Exception]
  def offsetCommitAsync(): Unit = {
    logger.info("async commit offset")

    kafkaConsumer.commitAsync(defaultConsumeRecordOffsetCommitCallback)
  }

  @throws[Exception]
  def offsetCommitAsync(fn: (util.Map[TopicPartition, OffsetAndMetadata], Exception) => Unit): Unit = {
    logger.info("async commit offset")

    kafkaConsumer.commitAsync(new OffsetCommitCallback {
      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
        fn(offsets, exception)
        throw exception
      }
    })
  }

  @throws[Exception]
  def close(): Unit = {
    logger.info("close kafka consumer client.")

    kafkaConsumer.close()
  }
}

object ConsumerClient extends LazyLogging {
  private val defaultKafkaConsumerClientPrefix = AppConfig.getKafkaClientPrefix(KafkaClientType.consumer)
  private val kafkaConsumerClientIdNum = new AtomicInteger(1)

  def apply[K, V](kafkaConsumer: Consumer[K, V], props: Properties): ConsumerClient[K, V] = {
    logger.debug("create consumer client.")
    new ConsumerClient(kafkaConsumer, props)
  }

  def apply[K, V](props: Properties): ConsumerClient[K, V] = {
    val copyProps = AppConfig.copyProperties(props)
    this.setAutoIncrementDefaultClientId(copyProps)

    this.apply(this.createKafkaConsumerClient[K, V](copyProps), copyProps)
  }

  def apply[K, V](props: Properties, groupId: String): ConsumerClient[K, V] = {
    val copyProps = AppConfig.copyProperties(props)
    copyProps.setProperty("group.id", groupId)
    this.setAutoIncrementDefaultClientId(copyProps)

    this.apply(this.createKafkaConsumerClient[K, V](copyProps), copyProps)
  }

  def apply[K, V](props: Properties, groupId: String, clientId: String): ConsumerClient[K, V] = {
    val copyProps = AppConfig.copyProperties(props)
    copyProps.setProperty("group.id", groupId)
    copyProps.setProperty("client.id", clientId)

    this.apply(this.createKafkaConsumerClient[K, V](copyProps), copyProps)
  }

  private def createKafkaConsumerClient[K, V](props: Properties): KafkaConsumer[K, V] = {
    logger.info("create kafka consumer client.")
    logger.info("kafka consumer client config:\n\t" + props.mkString("\n\t"))

    new KafkaConsumer[K, V](props)
  }

  def initOffset(): Unit = {
    // offset handling when initialize consumer client
    // delegate offset processing to the broker
  }

  private def setAutoIncrementDefaultClientId(props: Properties): Unit = {
    props.setProperty("client.id",
      props.getOrDefault("client.id", defaultKafkaConsumerClientPrefix)
        + "-" + kafkaConsumerClientIdNum.getAndIncrement())

    logger.debug(s"set kafka consumer client id. client.id: ${props.getProperty("client.id")}")
  }

  private def createDefaultConsumeRecordOffsetCommitCallBack: OffsetCommitCallback = new OffsetCommitCallback {
    override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
      if (exception == null) {
        logger.debug(s"async commit success, offsets: $offsets")
      } else {
        logger.error(s"commit failed for offsets: $offsets", exception)
        throw exception
      }
    }
  }
}
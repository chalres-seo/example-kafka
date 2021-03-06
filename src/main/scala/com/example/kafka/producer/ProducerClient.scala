package com.example.kafka.producer

import java.util
import java.util.Properties
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger

import com.example.kafka.metrics.KafkaMetrics
import com.example.utils.AppConfig
import com.example.utils.AppConfig.KafkaClientType
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.PartitionInfo

import scala.collection.JavaConversions._

/**
  * kafka producer client implements class.
  *
  * @see [[KafkaProducer]]
  * @see [[MockProducer]]
  *
  * @param kafkaProducer kafka producer client.
  * @param props kafka producer properties.
  * @tparam K kafka producer key serializer.
  * @tparam V kafka producer value serializer.
  */
class ProducerClient[K, V](kafkaProducer: Producer[K, V], props: Properties) extends LazyLogging {
  private val defaultProduceRecordCallback = ProducerClient.createDefaultProduceRecordCallBack
  private val producerMetrics = KafkaMetrics(kafkaProducer.metrics())

  def getClientId: String = props.getProperty("client.id")
  def getMetrics: KafkaMetrics = this.producerMetrics

  @throws[Exception]
  def sendProducerRecord(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    logger.debug(s"produce record to kafka. record: $record")

    kafkaProducer.send(record, defaultProduceRecordCallback)
  }

  @throws[Exception]
  def sendProduceRecord(record: ProducerRecord[K, V], producerCallback: Callback): Future[RecordMetadata] = {
    logger.debug(s"produce record to kafka. record: $record")

    kafkaProducer.send(record, producerCallback)
  }

  @throws[Exception]
  def sendProduceRecordWithCallback(record: ProducerRecord[K, V])
                                   (fn: (RecordMetadata, Exception) => Unit): Future[RecordMetadata] = {
    logger.debug(s"produce single record to kafka. record: $record")

    kafkaProducer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        fn(metadata, exception)
      }
    })
  }

  @throws[Exception]
  def sendProducerRecords(records: Vector[ProducerRecord[K, V]]): Vector[Future[RecordMetadata]] = {
    logger.debug(s"produce records to kafka. record count: ${records.length}")
    records.map(this.sendProducerRecord)
  }

  @throws[Exception]
  def sendProducerRecords(records: Vector[ProducerRecord[K, V]], producerCallBack: Callback): Vector[Future[RecordMetadata]] = {
    logger.debug(s"produce records to kafka. record count: ${records.length}")
    records.map(record => this.sendProduceRecord(record, producerCallBack))
  }

  @throws[Exception]
  def sendProducerRecordsWithCallback(records: Vector[ProducerRecord[K, V]])
                                     (fn: (RecordMetadata, Exception) => Unit): Vector[Future[RecordMetadata]] = {
    records.map(this.sendProduceRecordWithCallback(_)(fn))
  }

  def getPartitionInfo(topicName: String): util.List[PartitionInfo] = {
    logger.debug("get partition info.")
    kafkaProducer.partitionsFor(topicName)
  }

  def flush(): Unit = {
    logger.info("flush kafka producer client.")
    kafkaProducer.flush()
  }

  def close(): Unit = {
    logger.info("close kafka producer client.")
    kafkaProducer.close()
  }
}

object ProducerClient extends LazyLogging {
  private val defaultKafkaProducerClientPrefix = AppConfig.getKafkaClientPrefix(KafkaClientType.producer)
  private val kafkaProducerClientIdNum = new AtomicInteger(1)

  /** constructor */
  def apply[K, V](kafkaProducer: Producer[K, V], props: Properties): ProducerClient[K, V] = {
    logger.info("create [ProducerClient].")
    new ProducerClient(kafkaProducer, props)
  }

  /** constructor overload */
  def apply[K, V](props: Properties, clientId: String): ProducerClient[K, V] = {
    val copyProps = AppConfig.copyProperties(props)
    copyProps.setProperty("client.id", clientId)

    this.apply(this.createKafkaProducerClient[K, V](copyProps), copyProps)
  }

  /** constructor overload */
  def apply[K, V](props: Properties): ProducerClient[K, V] = {
    this.apply(props, this.getAutoIncrementClientId(props))
  }

  /** kafka producer client factory api */
  private def createKafkaProducerClient[K, V](props: Properties): KafkaProducer[K, V] = {
    logger.info(s"create [KafkaProducer]. client.id: ${props.getProperty("client.id")}")
    logger.info("[KafkaProducer] configs:\n\t" + props.mkString("\n\t"))
    new KafkaProducer[K, V](props)
  }

  private def getAutoIncrementClientId(props: Properties): String = {
    props.getOrDefault("client.id", defaultKafkaProducerClientPrefix) + "-" + kafkaProducerClientIdNum.getAndIncrement()

  }

  private def createDefaultProduceRecordCallBack: Callback = {
    logger.debug("create default producer record call back.")

    new Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) {
          logger.debug(s"succeed send record. metadata: ${ProducerClient.produceRecordMetadataToMap(metadata)}")
        } else {
          logger.error(s"failed send record. metadata: ${ProducerClient.produceRecordMetadataToMap(metadata).mkString("\n\t")}", exception)
          throw exception
        }
      }
    }
  }

  def produceRecordMetadataToMap(recordMetadata: RecordMetadata): Map[String, String] = {
    Map(
      "offset" -> recordMetadata.offset().toString,
      "partition" -> recordMetadata.partition().toString,
      "timestamp" -> recordMetadata.timestamp().toString,
      "topic" -> recordMetadata.topic()
    )
  }
}
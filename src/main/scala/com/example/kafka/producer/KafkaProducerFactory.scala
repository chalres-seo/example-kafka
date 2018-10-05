package com.example.kafka.producer

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, MockProducer}
import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

private[producer] object KafkaProducerFactory extends LazyLogging {
  private val kafkaProducerProps: Properties = AppConfig.getKafkaProducerProps
  private val kafkaProducerClientIdPrefix = kafkaProducerProps.getProperty("client.id")

  private val clientIdNum = new AtomicInteger(0)

  def createProducer[K, V](keySerializer: Serializer[K], valueSerializer: Serializer[V]): KafkaProducer[K, V] = {
    kafkaProducerProps.setProperty("client.id",kafkaProducerClientIdPrefix + "-" + clientIdNum.getAndIncrement())
    logger.debug("create kafka producer.")
    logger.debug("kafka producer props: \n" + kafkaProducerProps.mkString("\n"))
    new KafkaProducer(kafkaProducerProps, keySerializer, valueSerializer)
  }

  def createMockProducer[K, V](keySerializer: Serializer[K], valueSerializer: Serializer[V]): MockProducer[K, V] = {
    logger.debug("create kafka mock producer.")
    new MockProducer(true, keySerializer, valueSerializer)
  }
}

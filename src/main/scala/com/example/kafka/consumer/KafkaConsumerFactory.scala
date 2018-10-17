package com.example.kafka.consumer

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import com.example.utils.AppConfig
import org.apache.kafka.clients.consumer.{KafkaConsumer, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.serialization.Deserializer

private[kafka] object KafkaConsumerFactory {
  private val kafkaConsumerProps: Properties = AppConfig.getKafkaConsumerProps
  private val kafkaConsumerClientIdPrefix = kafkaConsumerProps.getProperty("client.id")

  private val clientIdNum = new AtomicInteger(1)

  def createConsumer[K, V](keyDeserializer: Deserializer[K],
                           valueDeserializer: Deserializer[V],
                           offsetResetStrategy: OffsetResetStrategy = OffsetResetStrategy.LATEST): KafkaConsumer[K, V] = {
    kafkaConsumerProps.setProperty("client.id", kafkaConsumerClientIdPrefix + "-" + clientIdNum.getAndIncrement())
    kafkaConsumerProps.setProperty("auto.offset.reset", offsetResetStrategy.toString.toLowerCase)
    new KafkaConsumer(kafkaConsumerProps, keyDeserializer, valueDeserializer)
  }

  def createMockConsumer[K, V](offsetResetStrategy: OffsetResetStrategy = OffsetResetStrategy.LATEST): MockConsumer[K, V] = {
    new MockConsumer[K, V](offsetResetStrategy)
  }
}

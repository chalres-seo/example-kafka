package com.example.kafka.consumer

import java.util.Scanner

import com.example.utils.AppConfig
import org.apache.kafka.clients.consumer.{KafkaConsumer, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.serialization.Deserializer

private[consumer] object KafkaConsumerFactory {
  private val kafkaConsumerProps = AppConfig.getKafkaConsumerProps

  def createConsumer[K, V](keyDeserializer: Deserializer[K],
                           valueDeserializer: Deserializer[V]): KafkaConsumer[K, V] = {
    new KafkaConsumer(kafkaConsumerProps, keyDeserializer, valueDeserializer)
  }

  def createMockConsumer[K, V](offsetResetStrategy: OffsetResetStrategy = OffsetResetStrategy.LATEST): MockConsumer[K, V] = {
    new MockConsumer[K, V](offsetResetStrategy)
  }
}

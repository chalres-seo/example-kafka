package com.example.kafka.admin

import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, KafkaAdminClient}

object KafkaAdminFactory extends LazyLogging {
  private val kafkaAdminProps = AppConfig.getKafkaAdminProps

  def createAdminClient(): KafkaAdminClient = {
    logger.debug("create kafka admin client")
    AdminClient.create(kafkaAdminProps).asInstanceOf[KafkaAdminClient]
  }
}

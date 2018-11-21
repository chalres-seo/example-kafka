package com.example.utils

import java.io.File
import java.nio.file.{Files, Paths}
import java.util
import java.util.Properties

import com.example.utils.AppConfig.KafkaClientType.KafkaClientType
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConversions._

/**
  * Application Config.
  */
object AppConfig extends LazyLogging {
  private val confPath = "conf/application.conf"
  private val conf: Config = this.createConfigFromFile(confPath)

  lazy val DEFAULT_KAFKA_ADMIN_PROPS: Properties = this.createDefaultKafkaAdminProps
  lazy val DEFAULT_KAFKA_PRODUCER_PROPS: Properties = this.createDefaultKafkaProducerProps
  lazy val DEFAULT_KAFKA_CONSUMER_PROPS: Properties = this.createDefaultKafkaConsumerProps

  val DEFAULT_EXECUTOR_SERVICE_THREAD_COUNT: Int = Runtime.getRuntime.availableProcessors() * 2

  private def createConfigFromFile(confFilePath: String): Config = {
    logger.info(s"read config file from: $confFilePath")
    ConfigFactory.parseFile(new File(confFilePath)).resolve()
  }

  private def createPropertiesFromFile(filePath: String): Properties = {
    logger.info(s"read properties file from : $filePath")
    val props: Properties = new Properties()
    props.load(Files.newInputStream(Paths.get(filePath)))
    props
  }

  def getApplicationName: String = this.conf.getString("application.name")

  def createDefaultKafkaAdminProps: Properties = this.createPropertiesFromFile(conf.getString("kafka.admin.props.file"))
  def createDefaultKafkaProducerProps: Properties = this.createPropertiesFromFile(conf.getString("kafka.producer.props.file"))
  def createDefaultKafkaConsumerProps: Properties = this.createPropertiesFromFile(conf.getString("kafka.consumer.props.file"))

  def getKafkaClientPrefix(clientType: KafkaClientType): String = conf.getString(s"kafka.prefix.${clientType.toString}.client.id")

  def copyProperties(sourceProps: Properties): Properties = {
    logger.debug("copy properties.")

    val newProperties = new Properties()
    sourceProps.propertyNames().foreach(name => newProperties.put(name.toString, sourceProps.get(name)))

    newProperties
  }

  object KafkaClientType extends Enumeration {
    type KafkaClientType = Value
    val admin, producer, consumer = Value
  }
}
package com.example.utils

import java.io.{File, FileInputStream, InputStream}
import java.nio.file.{Files, Paths}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

object AppConfig extends LazyLogging {
  private val conf: Config = this.readApplicationConfig("conf/application.conf")
  logger.debug(s"applicaion conf: ${conf.toString}")

  private val kafkaProducerProps = this.createKafkaProduceProps
  logger.debug(s"kafka producer props: ${kafkaProducerProps.toString}")

  private val kafkaConsumerProps = this.createKafkaConsumerProps

  def getApplicationName: String = conf.getString("application.name")

  def getKafkaProducerPropsFilePath: String = conf.getString("kafka.producer.props.path")
  def getKafkaConsumerPropsFilePath: String = conf.getString("kafka.consumer.props.path")

  def getKafkaProducerProps: Properties = this.kafkaProducerProps
  def getKafkaConsumerProps: Properties = this.kafkaConsumerProps

  def getKafkaTopic: String = conf.getString("kafka.topic")

  private def readApplicationConfig(confFilePath: String): Config = {
    logger.debug(s"read application config from: $confFilePath")
    ConfigFactory.parseFile(new File(confFilePath)).resolve()
  }

  private def createKafkaProduceProps: Properties = {
    logger.debug(s"create kafka producer props from: ${this.getKafkaProducerPropsFilePath}")
    val props: Properties = new Properties()

    props.load(Files.newInputStream(Paths.get(this.getKafkaProducerPropsFilePath)))
    props
  }

  private def createKafkaConsumerProps: Properties = {
    logger.debug(s"create kafka consumer props from: ${this.getKafkaConsumerPropsFilePath}")
    val props: Properties = new Properties()

    props.load(Files.newInputStream(Paths.get(this.getKafkaConsumerPropsFilePath)))
    props
  }

  def getAvroSchemaFileRootPath: String = conf.getString("avro.schema.root.path")
}
package com.example.apps

import java.lang

import com.example.kafka.admin.AdminClient
import com.example.kafka.consumer.ConsumerClient
import com.example.kafka.producer.ProducerClient
import com.example.utils.{AppConfig, AppUtils}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.joda.time.DateTime

import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import scala.collection.JavaConversions._

/**
  * Example kafka producer/consumer with iris string data set.
  *
  * step:
  * 1. read iris string data from file (produceDataSetPath)
  * 2. produce read iris string data (produce topic name: example.string.iris)
  * 3. consume iris string data (consume topic name: example.string.iris)
  * 4. write consume data to file (consumeDataSetPath)
  *
  */
object ExampleKafkaClientAppMain extends LazyLogging {
//  private val scheduledExecutor = new ScheduledThreadPoolExecutor(4)

  private val produceDataSetPath = "example_source/iris/iris.data"
  private val consumeDataSetPath = s"example_out/iris.consume.${DateTime.now().toString("yyyy-MM-dd_HH-mm-ss")}"

  private val topicName = "example.string.iris"

  private val consumerProps = AppConfig.createDefaultKafkaConsumerProps
  consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")

  private val adminProps = AppConfig.createDefaultKafkaAdminProps

  private val producerProps = AppConfig.createDefaultKafkaProducerProps
  producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")

  private val consumerClient = ConsumerClient[Int, String](consumerProps)
  consumerClient.subscribeTopic(topicName)

  private val adminClient = AdminClient(adminProps)
  private val producerClient = ProducerClient[Int, String](producerProps)

  def main(args: Array[String]): Unit = {
    logger.info("start example kafka client app main.")

    this.prepareExample()

    val consumerTaskFuture: Future[Unit] = this.startConsumerTask(3000L, 30000L)
    logger.info("wait 3sec start consumer future.")
    Thread.sleep(3000)
    val producerTaskFuture: Future[Unit] = this.startProducerTask(3000L, 20000L)
    logger.info("wait 3sec start producer future.")
    Thread.sleep(3000)

    Await.result(producerTaskFuture, Duration.Inf)
    Await.result(consumerTaskFuture, Duration.Inf)

    logger.info("wait cleanup completed future.")
    Thread.sleep(3000)
    this.cleanUpExample()

    logger.info("finish example kafka client app main.")
  }

  def startProducerTask(produceIntervalTimeMills: Long, produceTimeMills: Long): Future[Unit] = {
    logger.info(s"start producer task. producer interval time millis: $produceIntervalTimeMills, producer time millis: $produceTimeMills")
    val endTimeMills = DateTime.now.getMillis + produceTimeMills

    Future {
      do {
        this.producerTask()
        logger.info(s"wait interval time millis. interval time millis: $produceIntervalTimeMills")
        Thread.sleep(produceIntervalTimeMills)
      } while (DateTime.now.getMillis < endTimeMills)
      logger.info("end producer task.")
    }
  }

  def producerTask(): Unit = {
    logger.info("produce example records.")
    Source.fromFile(produceDataSetPath).getLines().foreach { record =>
      producerClient.sendProducerRecord(stringToProducerRecord(topicName, record))
    }
  }

  def startConsumerTask(consumeIntervalTimeMills: Long, consumeTimeMills: Long): Future[Unit] = {
    logger.info(s"start consumer task. producer interval time millis: $consumeIntervalTimeMills, producer time millis: $consumeTimeMills")

    val endTimeMills = DateTime.now.getMillis + consumeTimeMills

    Future {
      do {
        this.consumerTask()
        logger.info(s"wait interval time millis. interval time millis: $consumeIntervalTimeMills")
        Thread.sleep(consumeIntervalTimeMills)
      } while (DateTime.now.getMillis < endTimeMills)
      logger.info("end consumer task.")
    }
  }

  def consumerTask(): Unit = {
    logger.info("consume example records.")

    val consumerRecords = consumerClient.consumeRecord

    if (consumerRecords.isEmpty) {
      logger.info("consume record is empty.")
      consumerClient.offsetCommitAsync()
    } else {
      logger.info(s"consume record count: ${consumerRecords.count()}")
      AppUtils.writeFile(consumeDataSetPath, consumerRecords.records(topicName).map(_.value()).toVector, append = true)
      consumerClient.offsetCommitAsync()
    }
  }

  def stringToProducerRecord(topicName: String, record: String): ProducerRecord[Int, String] = {
    new ProducerRecord[Int, String](topicName, record.hashCode, record)
  }

  def stringListToProducerRecordList(topicName: String, records: Vector[String]): Vector[ProducerRecord[Int, String]] = {
    records.map(record => stringToProducerRecord(topicName, record))
  }

  /** prepare resource */
  def prepareExample(): Unit = {
    /** create topic */
    if (!adminClient.isExistTopic(topicName)) {
      adminClient.createTopic(topicName, 3, 3).get
      while(!adminClient.isExistTopic(topicName)) {
        adminClient.createTopic(topicName, 3, 3).get
        Thread.sleep(500)
      }
    }
  }

  /** clean up resource */
  def cleanUpExample(): Unit = {
    producerClient.flush()
    producerClient.close()

    consumerClient.unsubscribeAllTopic()
    consumerClient.close()

    /** delete topic */
    if (adminClient.isExistTopic(topicName)) {
      adminClient.deleteTopic(topicName).get
      while(adminClient.isExistTopic(topicName)) {
        adminClient.deleteTopic(topicName).get
        Thread.sleep(500)
      }
    }
    adminClient.close()
  }
}

package com.example.apps

import com.example.kafka.admin.AdminClient
import com.example.kafka.{consumer, producer}
import com.example.utils.{AppConfig, AppUtils}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.joda.time.DateTime

import scala.io.Source
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Example kafka producer/consumer worker with iris string data set.
  *
  * step:
  * 1. start consumer worker.
  * 2. start producer worker.
  * 3. read iris string data from file (readDataSetPath)
  * 5. 2 times iris string data offer to producer buffer (produce topic name: example.worker.string.iris)
  * 3. iris string data poll from consumer buffer (consume topic name: example.worker.string.iris)
  * 4. write polled data to file (writeDataSetPath)
  *
  */
object ExampleKafkaClientWorkerAppMain extends LazyLogging {
  private val readDataSetPath = "example_source/iris/iris.data"
  private val writeDataSetPath = s"example_out/iris.consume.${DateTime.now().toString("yyyy-MM-dd_HH-mm-ss")}"

  private val topicName = "example.worker.string.iris"

  private val consumerProps = AppConfig.createDefaultKafkaConsumerProps
  consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")

  private val adminProps = AppConfig.createDefaultKafkaAdminProps

  private val producerProps = AppConfig.createDefaultKafkaProducerProps
  producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")

  private val adminClient = AdminClient(adminProps)

  private val consumerWorker = consumer.ConsumerWorker[Int, String](consumerProps, topicName)
  private val producerWorker = producer.ProducerWorker[Int, String](producerProps)

  def main(args: Array[String]): Unit = {
    consumerWorker.start()
    logger.info("wait 3sec for start consumer worker.")
    Thread.sleep(3000)

    producerWorker.start()
    logger.info("wait 3sec for start producer worker.")
    Thread.sleep(3000)

    logger.info("example data offer to producer buffer and wait 3sec for each processing.")
    writeProducerBuffer()
    Thread.sleep(3000)
    writeProducerBuffer()
    Thread.sleep(3000)

    val producerWorkerStopFuture = producerWorker.stop()

    logger.info("polling from consume buffer.")

    while(!consumerWorker.isBufferEmpty) {
      readConsumerBuffer.foreach { consumerRecords =>
        AppUtils.writeFile(writeDataSetPath, consumerRecords.map(_.value()), append = true)
      }
    }
    val consumerWorkerStopFuture = consumerWorker.stop()

    Await.result(producerWorkerStopFuture, Duration.Inf)
    Await.result(consumerWorkerStopFuture, Duration.Inf)
  }

  def writeProducerBuffer(): Unit = {
    logger.info("produce example records.")
    Source.fromFile(readDataSetPath).getLines().foreach { record =>
      producerWorker.offerProducerRecordToBuffer(this.stringToProducerRecord(topicName, record))
    }
  }

  def readConsumerBuffer: Option[Vector[ConsumerRecord[Int, String]]] = {
    logger.info("consume records.")
    consumerWorker.getAllConsumerRecordsFromBuffer.map { consumerRecordsList =>
      consumerRecordsList.flatMap(_.iterator())
    }
  }

  def stringToProducerRecord(topicName: String, record: String): ProducerRecord[Int, String] = {
    new ProducerRecord[Int, String](topicName, record.hashCode, record)
  }

  /** prepare resource */
  def prepareExample(): Unit = {
    /** create topic */
    if (!adminClient.isExistTopic(topicName)) {
      adminClient.createTopic(topicName, 3, 3).get
      while(!adminClient.isExistTopic(topicName)) {
        Thread.sleep(500)
        adminClient.createTopic(topicName, 3, 3).get
      }
    }
  }

  /** clean up resource */
  def cleanUpExample(): Unit = {
    /** delete topic */
    if (adminClient.isExistTopic(topicName)) {
      adminClient.deleteTopic(topicName).get
      while(adminClient.isExistTopic(topicName)) {
        Thread.sleep(500)
        adminClient.deleteTopic(topicName).get
      }
    }
    adminClient.close()
  }
}

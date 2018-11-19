//package com.example.kafka.consumer
//
//
//import com.example.kafka.admin.AdminClient
//import com.typesafe.scalalogging.LazyLogging
//import org.apache.kafka.clients.consumer._
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
//import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
//import java.time.{Duration => JDuration}
//import java.util
//import java.util.concurrent.{Future => JFuture}
//
//import com.example.utils.AppConfig
//import org.apache.kafka.common.TopicPartition
//
//import scala.collection.JavaConversions._
//import scala.annotation.tailrec
//import org.junit.{After, Assert, Before, Test}
//import org.hamcrest.CoreMatchers._
//
//import scala.concurrent.Await
//import scala.concurrent.duration.Duration
//
//class TestConsumerWorker extends LazyLogging {
//  private val testTopicName: String = "test-consumer"
//  private val testTopicPartitionCount: Int = 3
//  private val testTopicReplicaFactor:Short = 3
//
//  private val testTopicRecordSetCount = 10
//  private val testRecordSet: Vector[ProducerRecord[String, String]] = (1 to testTopicRecordSetCount)
//    .map(index => new ProducerRecord[String, String](testTopicName, s"key-$index", s"value-$index"))
//    .toVector
//
//  private val kafkaAdmin = AdminClient()
//
//  private lazy val commitAsyncCallback = this.getConsumerCommitAsyncCallback
//
//  @Before
//  def setUp(): Unit = {
//    while (kafkaAdmin.isExistTopic(testTopicName)) {
//      kafkaAdmin.deleteTopic(testTopicName)
//      Thread.sleep(1000)
//    }
//
//    while(!kafkaAdmin.isExistTopic(testTopicName)) {
//      kafkaAdmin.createTopic(testTopicName, testTopicPartitionCount, testTopicReplicaFactor)
//      Thread.sleep(1000)
//    }
//  }
//
//  @After
//  def cleanUp(): Unit = {
//    while (kafkaAdmin.isExistTopic(testTopicName)) {
//      kafkaAdmin.deleteTopic(testTopicName)
//      Thread.sleep(1000)
//    }
//  }
//
//  @Test
//  def testConsumerFromBeginning(): Unit = {
//    this.produceTestRecordSet()
//
//    val kafkaConsumer: KafkaConsumer[String, String] =
//      KafkaConsumerFactory.createConsumer(new StringDeserializer, new StringDeserializer, OffsetResetStrategy.EARLIEST)
//    kafkaConsumer.subscribe(util.Collections.singletonList(testTopicName))
//
//    @tailrec
//    def loop(records:Vector[ConsumerRecord[String, String]]):Vector[ConsumerRecord[String, String]] = {
//      val consumeRecords: Vector[ConsumerRecord[String, String]] =
//        kafkaConsumer.poll(JDuration.ofMillis(1000)).iterator().toVector
//
//      if (consumeRecords.isEmpty) {
//        records
//      } else {
//        kafkaConsumer.commitAsync(commitAsyncCallback)
//        loop(records ++ consumeRecords)
//      }
//    }
//
//    val records = loop(kafkaConsumer.poll(JDuration.ofMillis(1000)).iterator().toVector)
//    logger.debug(records.mkString("\n"))
//
//    kafkaConsumer.close()
//    Assert.assertThat(records.length, is(testTopicRecordSetCount))
//  }
//
//  @Test
//  def testConsumerWorker(): Unit = {
//    val consumerWorker: ConsumerWorker[String, String] =
//      ConsumerWorker(testTopicName, new StringDeserializer, new StringDeserializer)
//
//    consumerWorker.start()
//    Thread.sleep(3000)
//    this.produceTestRecordSet()
//    Thread.sleep(3000)
//
//    val records = consumerWorker.getConsumerRecords
//    records.foreach(records => records.iterator().foreach(println))
//
//    Await.result(consumerWorker.stop(), Duration.Inf)
//
//    Assert.assertThat(records.map(_.count()).sum, is(testTopicRecordSetCount))
//    consumerWorker.close()
//  }
//
//  @Test
//  def testConsumerWorkerFromBeginning(): Unit = {
//    this.produceTestRecordSet()
//
//    val consumerWorker: ConsumerWorker[String, String] =
//      ConsumerWorker(testTopicName, new StringDeserializer, new StringDeserializer, OffsetResetStrategy.EARLIEST)
//
//    consumerWorker.start()
//    Thread.sleep(3000)
//
//    val records = consumerWorker.getConsumerRecords
//    records.foreach(records => records.iterator().foreach(println))
//
//    Await.result(consumerWorker.stop(), Duration.Inf)
//
//    Assert.assertThat(records.map(_.count()).sum, is(testTopicRecordSetCount))
//    consumerWorker.close()
//  }
//
//
//  def produceTestRecordSet(): Unit = {
//    val kafkaProducer = new KafkaProducer(AppConfig.getKafkaProducerProps, new StringSerializer(), new StringSerializer())
//
//    val produceFutures: Vector[JFuture[RecordMetadata]] = testRecordSet.map(kafkaProducer.send)
//    while(!produceFutures.forall(_.isDone)) { Thread.sleep(1000) }
//
//    kafkaProducer.flush()
//
//    logger.debug {
//      produceFutures.map { f =>
//        val metadata = f.get()
//        s"succeed send record. metadata: " +
//          "topic: " + metadata.topic() + ", " +
//          "partition: " + metadata.partition() + ", " +
//          "offset: " + metadata.offset() + ", " +
//          "timestamp: " + metadata.timestamp() + ", " +
//          "serialized key size: " + metadata.serializedKeySize() + ", " +
//          "serialized value size: " + metadata.serializedValueSize()
//      }.mkString("\n")
//    }
//
//    kafkaProducer.close()
//  }
//
//  def getConsumerCommitAsyncCallback: OffsetCommitCallback = {
//    new OffsetCommitCallback {
//      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
//        if (exception == null) {
//          logger.debug(s"async commit success, offsets: $offsets")
//        } else logger.error(s"commit failed for offsets: $offsets", exception)
//      }
//    }
//  }
//}

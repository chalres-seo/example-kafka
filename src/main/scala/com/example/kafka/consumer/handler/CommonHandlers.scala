//package com.example.kafka.consumer.handler
//
//import ch.qos.logback.classic.{Level, Logger}
//import com.example.kafka.producer.ProducerWorker
//import com.typesafe.scalalogging.LazyLogging
//import org.apache.kafka.clients.consumer.ConsumerRecords
//import org.apache.kafka.clients.producer.ProducerRecord
//import org.slf4j.LoggerFactory
//
//import scala.collection.JavaConversions._
//import scala.concurrent.Future
//import scala.concurrent.ExecutionContext.Implicits.global
//
//object CommonHandlers extends LazyLogging {
//  def printStdout[K, V](records: ConsumerRecords[K, V]): Future[Unit] = {
//    Future {
//      records.foreach(println)
//    }
//  }
//
//  def printDebug[K, V](records: ConsumerRecords[K, V]): Future[Unit] = {
//    Future {
//      if (LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].getLevel == Level.DEBUG) {
//        records.foreach(record => logger.debug(record.toString))
//      }
//    }
//  }
//
//  def toOtherKafka[K, V](producerWorker: ProducerWorker[K, V], producerTopicName: String, records: ConsumerRecords[K, V]): Future[Unit] = {
//    Future {
//      producerWorker.addProduceRecords(records.map {
//        record => new ProducerRecord(producerTopicName, record.key(), record.value())
//      }.toVector)
//    }
//  }
//}

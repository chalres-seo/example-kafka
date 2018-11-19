//package com.example.kafka.consumer.handler
//
//import com.typesafe.scalalogging.LazyLogging
//import org.apache.kafka.clients.consumer.ConsumerRecords
//
//import scala.collection.JavaConversions._
//import scala.concurrent.Future
//import scala.concurrent.ExecutionContext.Implicits.global
//
//
//object StdoutHandler extends LazyLogging with IConsumerRecordHandler {
//  override def processConsumeRecord[K, V](records: ConsumerRecords[K, V]): Future[Unit] = {
//    Future {
//      records.foreach(println)
//    }
//  }
//}
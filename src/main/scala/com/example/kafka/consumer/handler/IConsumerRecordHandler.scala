//package com.example.kafka.consumer.handler
//
//import org.apache.kafka.clients.consumer.ConsumerRecords
//
//import scala.concurrent.Future
//
//trait IConsumerRecordHandler {
//  def processConsumeRecord[K, V](records: ConsumerRecords[K, V]): Future[Unit]
//}
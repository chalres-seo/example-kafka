# Example-kafka-lib-scala

Example Scala library for [Kafka] version 2.x : Admin, Producer, Consumer.

* This example does not cover advanced topics.
* This example follow 'at least once' consumer processing rule.
* Topic offset control by kafka broker.

## Kafka Introduction

Apache Kafka is a distributed streaming platform. [[Kafka Introduction]]

* Topics and Logs
* Distribution
* Replication
* Producer
* Consumer

## Futures

* [Kafka Admin Client API]
* [Kafka Producer Client API]
* [Kafka Consumer Client APi]
* Kafka Producer Worker
* Kafka Consumer Worker

## Usage

#### Admin Client API

Need admin client properties.
Default admin client properties read from kafka.admin.props.file property in 'conf/application.conf' 

* Constructor
*
*

#### Producer Client API

Need producer client properties.
Default producer client properties read from kafka.producer.props.file property in 'conf/application.conf'

* Constructor
*
*

#### Consumer Client API

Need consumer client properties.
Default consumer client properties read from kafka.consumer.props.file property in 'conf/application.conf'

* Constructor
*
*

#### Producer Worker

* Concept

The worker waits for the ProducerRecord to arrive in the buffer and consume buffer ProducerReocrd and send it to kafka.

* Constructor
*
*

#### Consumer Worker

* Concept

The worker polling ConsumerRecords from kafka broker and produce ConsumerRecords to buffer.
  
* Constructor
*
*

## Kafka mini-cluster docker

* Sandbox-centos-openjdk8
* Sandbox-kafka


## Build with test

./gradlew build

## Example

#### Example source
 
* iris data : http://archive.ics.uci.edu/ml/datasets/Iris

#### Example out

* ./example_out/iris.consume.{yyyy-MM-dd_HH:mm:ss} 

#### Example app main

* com.example.ExampleKafkaClientAppMain
* com.example.ExampleKafkaClientWorkerAppMain

#### Run example by gradle

* ExampleKafkaClientAppMain : ./gradlew task api
* ExampleKafkaClientWorkerAppMain : ./gradlew task worker


[Kafka]: https://kafka.apache.org/
[Kafka Introduction]: https://kafka.apache.org/intro.html
[Kafka Admin Client API]: https://kafka.apache.org/documentation/#adminapi
[Kafka Producer Client API]: https://kafka.apache.org/documentation/#producerapi 
[Kafka Consumer Client API]: https://kafka.apache.org/documentation/#consumerapi
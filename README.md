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
* [Kafka Producer Worker]
* [Kafka Consumer Worker]

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
* Constructor
*
*

#### Consumer Worker

* Concept
* Constructor
*
*

## Kafka mini-cluster docker

* sandbox kafka


## Build

#### Build skip test

## Test run

#### Run example by gradle

#### Run example by jar

[Kafka]: https://kafka.apache.org/
[Kafka Introduction]: https://kafka.apache.org/intro.html
[Kafka Admin Client API]: https://kafka.apache.org/documentation/#adminapi
[Kafka Producer Client API]: https://kafka.apache.org/documentation/#producerapi 
[Kafka Consumer Client API]: https://kafka.apache.org/documentation/#consumerapi
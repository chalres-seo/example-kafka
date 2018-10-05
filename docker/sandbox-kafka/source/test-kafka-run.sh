#!/bin/bash

test_kafka_home_dir=/home/kafka/kafka_2.11-2.0.0
test_kafka_conf_dir=${test_kafka_home_dir}/test-kafka-conf

date_time=`date +'%Y%m%d_%H%M%S'`

echo "start zookeeper."
${test_kafka_home_dir}/bin/zookeeper-server-start.sh ${test_kafka_conf_dir}/zookeeper.properties \
 1> ${test_kafka_home_dir}/logs/sandbox-kafka_zookeeper_${date_time}.log \
 2> ${test_kafka_home_dir}/logs/sandbox-kafka_zookeeper_${date_time}.err &

echo "start kafka broker-1"
${test_kafka_home_dir}/bin/kafka-server-start.sh ${test_kafka_conf_dir}/server-1.properties \
 1> ${test_kafka_home_dir}/logs/sandbox-kafka_server_1_${date_time}.log \
 2> ${test_kafka_home_dir}/logs/sandbox-kafka_server_1_${date_time}.err &

echo "start kafka broker-2"
${test_kafka_home_dir}/bin/kafka-server-start.sh ${test_kafka_conf_dir}/server-2.properties \
 1> ${test_kafka_home_dir}/logs/sandbox-kafka_server_2_${date_time}.log \
 2> ${test_kafka_home_dir}/logs/sandbox-kafka_server_2_${date_time}.err &

echo "start kafka broker-3"
${test_kafka_home_dir}/bin/kafka-server-start.sh ${test_kafka_conf_dir}/server-3.properties \
 1> ${test_kafka_home_dir}/logs/sandbox-kafka_server_3_${date_time}.log \
 2> ${test_kafka_home_dir}/logs/sandbox-kafka_server_3_${date_time}.err

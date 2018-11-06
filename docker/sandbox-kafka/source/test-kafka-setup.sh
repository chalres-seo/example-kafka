#!/bin/bash

kafka=kafka_2.11-2.0.0
kafka_bin_tar=/tmp/source/${kafka}.tgz
kafka_home=/home/kafka/${kafka}

echo ">> start test kafka setup script."
echo ""

echo ">> user add [kafka]."
useradd kafka
echo ">> done."
echo ""

echo ">> decompress kafka bin."
tar xf ${kafka_bin_tar} --directory /home/kafka
echo ">> done."
echo ""


echo ">> copy test kafka config and run script."
cp -r /tmp/source/test-kafka-conf/ ${kafka_home}
cp /tmp/source/test-kafka-run.sh ${kafka_home}/..
echo ">> done."
echo ""


echo ">> make runnung env dir. logs, data"
mkdir ${kafka_home}/logs
mkdir ${kafka_home}/data
echo ">> done."
echo ""


echo ">>set own permission for kafka."
chown -R kafka:kafka /home/kafka
echo ">> done."
echo ""

echo ">> add local ip for kafka."
echo ""
echo ">> complete test kafka setup script."

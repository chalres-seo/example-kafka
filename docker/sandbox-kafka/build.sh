kafka_bin_link_address=http://mirror.navercorp.com/apache/kafka/2.0.0/kafka_2.11-2.0.0.tgz
kafka_bin_tar=kafka_2.11-2.0.0.tgz
kafka_home_dir=/home/kafka/kafka_2.11-2.0.0

if [ -f "source/${kafka_bin_tar}" ]
then
    echo "exist kafka bin."
else
    echo "kafka bin not found, download bin"
    wget $kafka_bin_link_address
    mv $kafka_bin_tar source/
fi

docker build -t sandbox-kafka .

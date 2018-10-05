# base centos
FROM	sandbox-centos-openjdk8:latest
MAINTAINER	who@who.com

ADD source/ /tmp/source

RUN /tmp/source/test-kafka-setup.sh

USER kafka

CMD ["/home/kafka/test-kafka-run.sh"]

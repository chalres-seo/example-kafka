package com.example.kafka.metrics

import com.example.kafka.producer.ProducerWorker
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import org.junit.Test

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.Source

class TestProducerMetrics extends LazyLogging {
  private val testTopicName: String = "test"

  private val recordCount: Int = 1000
  private val recordSet: Vector[ProducerRecord[String, String]] = (1 to recordCount)
    .map(index => new ProducerRecord[String, String](testTopicName, s"key-$index", s"value-$index"))
    .toVector

  @Test
  def testProducerMetrics(): Unit = {
    val producerWorker: ProducerWorker[String, String] = ProducerWorker(new StringSerializer, new StringSerializer)
    val producerMetrics: KafkaMetrics = producerWorker.getKafkaProducerMetrics

    logger.info(s"before add $recordCount records to buffer: ${producerWorker.getBufferSize}")
    producerWorker.addProduceRecords(recordSet)
    while (producerWorker.getBufferSize != recordSet.length) {
      logger.info(s"wait 1sec for add test record to buffer complete. buffer size: ${producerWorker.getBufferSize}")
      Thread.sleep(1000)
    }
    logger.info(s"after add records to buffer: ${producerWorker.getBufferSize}")

    logger.info("start produce worker.")
    producerWorker.start()
    while (producerWorker.getBufferSize != 0 || producerWorker.getIncompleteAsyncProduceRecordCount != 0) {}
    logger.info("stop produce worker.")
    Await.result(producerWorker.stop(), Duration.Inf)

    logger.info(s"current buffer size: ${producerWorker.getBufferSize}")
    logger.info("check metrics function")

    logger.info("all metrics\n"
      + producerMetrics.getAllMetrics.take(10).mkString("\n")
      + "\n...")

    logger.info("get metrics by group: producer-metrics\n"
      + producerMetrics.getMetricsByGroup("producer-metrics").take(10).mkString("\n")
      + "\n...")

    logger.info("get metrics by name: record-send-rate\n"
      + producerMetrics.getMetricsByName("record-send-rate").take(10).mkString("\n")
      + "\n...")

    logger.info(s"get metric by group and name. group: ${"producer-metrics"}, name: record-send-total${}\n"
      + producerMetrics.getMetric("producer-metrics", "record-send-total").take(10).mkString("\n"))

    logger.info("scan metrics by group: kafka\n"
      + producerMetrics.scanMetricsByGroup("kafka").take(10).mkString("\n")
      + "\n...")

    logger.info("scan metrics by name: send\n"
      + producerMetrics.scanMetricsByName("send").take(10).mkString("\n")
      + "\n...")

    logger.info("scan metrics by group and name. group: producer, name: send\n"
      + producerMetrics.scanMetricsGroupAndName("producer", "send").take(10).mkString("\n")
      + "\n...")

    logger.info("scan metrics by group or name. group: producer, name: send\n"
      + producerMetrics.scanMetricsGroupOrName("kafka", "send").take(10).mkString("\n")
      + "\n...")

    logger.info("write all metrics.")
    producerMetrics.overwriteToFile("temp/all_metrics.csv", producerMetrics.getAllMetrics)

    logger.info("read metrics from file")
    Source.fromFile("temp/all_metrics.csv").getLines().foreach(println)
  }
}

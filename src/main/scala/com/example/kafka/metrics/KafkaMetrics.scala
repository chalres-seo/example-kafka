package com.example.kafka.metrics

import java.util

import com.example.utils.AppUtils
import org.apache.kafka.common.{Metric, MetricName}

import scala.collection.JavaConversions._

case class KafkaMetrics(metrics: util.Map[MetricName, _ <: Metric]) {

  def getAllMetrics: Iterable[KafkaMetric] = metrics.values().map(KafkaMetric.apply)

  def getGroupedMetrics: Map[String, Iterable[KafkaMetric]] = {
    metrics.values()
      .groupBy(_.metricName().group())
      .map(grouped => (grouped._1, grouped._2.map(KafkaMetric.apply)))
  }

  def getMetric(group: String, name: String): Iterable[KafkaMetric] = {
    this.findKeyAndGetValueToIter(metricName =>
      metricName.group() == group && metricName.name == name
    )
  }

  def getMetricsByGroup(group: String): Iterable[KafkaMetric] = {
    this.findKeyAndGetValueToIter(_.group() == group)
  }

  def getMetricsByName(name: String): Iterable[KafkaMetric] = {
    this.findKeyAndGetValueToIter(_.name() == name)
  }

  def scanMetricsGroupAndName(group: String, name: String): Iterable[KafkaMetric] = {
    this.findKeyAndGetValueToIter(metricName =>
      metricName.group().contains(group) && metricName.name.contains(name)
    )
  }

  def scanMetricsGroupOrName(group: String, name: String): Iterable[KafkaMetric] = {
    this.findKeyAndGetValueToIter(metricName =>
      metricName.group().contains(group) || metricName.name.contains(name)
    )
  }

  def scanMetricsByName(name: String): Iterable[KafkaMetric] = {
    this.findKeyAndGetValueToIter(_.name().contains(name))
  }

  def scanMetricsByGroup(group: String): Iterable[KafkaMetric] = {
    this.findKeyAndGetValueToIter(_.group().contains(group))
  }

  def appendToFile(appendFilePath: String, producerMetrics: Iterable[KafkaMetric]): Unit = {
    AppUtils.writeFile(appendFilePath, producerMetrics.map(_.toString).toVector, true)
  }

  def overwriteToFile(appendFilePath: String, producerMetrics: Iterable[KafkaMetric]): Unit = {
    AppUtils.writeFile(appendFilePath, producerMetrics.map(_.toString).toVector, false)
  }

  private def findKeyAndGetValueToIter(findKeyExpr: MetricName => Boolean): Iterable[KafkaMetric] = {
    metrics.keySet()
      .filter(findKeyExpr)
      .map(key => KafkaMetric.apply(metrics.get(key)))
      .toIterable
  }
}
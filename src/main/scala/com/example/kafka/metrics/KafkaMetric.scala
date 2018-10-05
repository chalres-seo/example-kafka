package com.example.kafka.metrics

import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.{Metric, MetricName}
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

case class KafkaMetric(dateTime: DateTime,
                       group: String,
                       name: String,
                       value: AnyRef,
                       tag: Map[String, String],
                       description: String) extends LazyLogging {

  override def toString: String = {
    Vector(
      dateTime.toString("yyyy-MM-dd HH:mm:ss"),
      group,
      name,
      Try(BigDecimal(value.toString).bigDecimal.toPlainString) match {
        case Success(plainBigDecimal) =>
          plainBigDecimal
        case Failure(_) =>
          logger.warn(s"change the non-numeric type metric value to a string. value: $value")
          value.toString
      }
      ,
      "{" + tag.map(e => s""""${e._1}" : "${e._2}"""").mkString(",") + "}",
      description
    ).mkString(",")
  }
}

object KafkaMetric {
  def apply(metric: Metric): KafkaMetric = {
    new KafkaMetric(DateTime.now(),
      metric.metricName.group(),
      metric.metricName.name,
      metric.metricValue(),
      metric.metricName.tags().toMap,
      metric.metricName.description())
  }
  //def toProduceMetric[T <: Metric](metric: T): ProducerMetric = this.apply(metric)
}
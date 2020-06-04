// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.monitoring.metrics

import com.ultimatesoftware.scala.core.kafka.KafkaStringProducer
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.Json

object MonitoringKafkaPublisher {
  def apply(producer: KafkaStringProducer): MonitoringKafkaPublisher = {
    MonitoringKafkaPublisher(producer.producer, producer.topic.name)
  }
}
final case class MonitoringKafkaPublisher(producer: KafkaProducer[String, String], topicName: String) extends MetricsPublisher {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  log.debug(s"Starting MonitoringKafkaPublisher for topic $topicName")

  override def publish(metrics: Seq[Metric]): Unit = {
    val metricsFormatted = metrics.map(Json.toJson(_).toString)
    metricsFormatted.foreach { metric ⇒
      producer.send(new ProducerRecord(topicName, metric))
    }
  }
}

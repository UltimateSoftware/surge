// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.monitoring.metrics

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.Json
import surge.metrics.{ Metric, MetricsPublisher }
import surge.scala.core.kafka.KafkaStringProducer

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
    metricsFormatted.foreach { metric =>
      producer.send(new ProducerRecord(topicName, metric))
    }
  }
}

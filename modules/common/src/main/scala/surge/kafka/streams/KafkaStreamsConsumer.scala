// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }
import surge.kafka.{ KafkaSecurityConfiguration, KafkaTopic }
import surge.kafka.streams.DefaultSerdes._

abstract class KafkaStreamsConsumer[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]) extends KafkaSecurityConfiguration {
  def brokers: Seq[String]
  def applicationId: String
  def kafkaConfig: Map[String, String]

  def applicationServerConfig: Option[String]

  def topologyProps: Option[Properties]

  private def props: Properties = {
    val p = new Properties()
    kafkaConfig.foreach(propPair => p.put(propPair._1, propPair._2))
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    applicationServerConfig.foreach(config => p.put(StreamsConfig.APPLICATION_SERVER_CONFIG, config))
    configureSecurityProperties(p)
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder

  def stream(topic: KafkaTopic): KStream[K, V] = {
    builder.stream[K, V](topic.name)
  }

  lazy val topology: Topology = {
    topologyProps.map(props => builder.build(props)).getOrElse(builder.build())
  }
  lazy val streams: KafkaStreams = new KafkaStreams(topology, props)

  def start(): Unit = {
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.of(10, ChronoUnit.SECONDS))
    }
  }
}

case class GenericKafkaStreamsConsumer[Value](
    brokers: Seq[String],
    applicationId: String,
    kafkaConfig: Map[String, String],
    applicationServerConfig: Option[String] = None,
    topologyProps: Option[Properties] = None)(implicit valueSerde: Serde[Value])
    extends KafkaStreamsConsumer[String, Value]

case class KafkaStringStreamsConsumer(
    brokers: Seq[String],
    applicationId: String,
    kafkaConfig: Map[String, String],
    applicationServerConfig: Option[String] = None,
    topologyProps: Option[Properties] = None)
    extends KafkaStreamsConsumer[String, String]

case class KafkaByteStreamsConsumer(
    brokers: Seq[String],
    applicationId: String,
    kafkaConfig: Map[String, String],
    applicationServerConfig: Option[String] = None,
    topologyProps: Option[Properties] = None)
    extends KafkaStreamsConsumer[String, Array[Byte]]

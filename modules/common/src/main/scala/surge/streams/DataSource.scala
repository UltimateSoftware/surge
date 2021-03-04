// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.streams

import java.util.Properties

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Flow, GraphDSL, Merge, Partition }
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory
import surge.akka.streams.kafka.{ KafkaConsumer, KafkaStreamManager }
import surge.metrics.Metrics
import surge.scala.core.kafka.KafkaTopic
import surge.streams.replay.{ DefaultEventReplaySettings, EventReplaySettings, EventReplayStrategy, NoOpEventReplayStrategy }

import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._
import scala.util.hashing.MurmurHash3

trait DataSource {
  def replayStrategy: EventReplayStrategy = NoOpEventReplayStrategy
  def replaySettings: EventReplaySettings = DefaultEventReplaySettings
}

trait KafkaDataSource[Key, Value] extends DataSource {
  private val config: Config = ConfigFactory.load()
  private val defaultBrokers = config.getString("kafka.brokers")

  def kafkaBrokers: String = defaultBrokers
  def kafkaTopic: KafkaTopic

  def actorSystem: ActorSystem

  def keyDeserializer: Deserializer[Key]
  def valueDeserializer: Deserializer[Value]

  def metrics: Metrics = Metrics.globalMetricRegistry

  def additionalKafkaProperties: Properties = new Properties()

  def to(sink: DataHandler[Key, Value], consumerGroup: String): DataPipeline = {
    to(sink, consumerGroup, autoStart = true)
  }

  def to(sink: DataHandler[Key, Value], consumerGroup: String, autoStart: Boolean): DataPipeline = {
    val consumerSettings = KafkaConsumer.consumerSettings[Key, Value](actorSystem, groupId = consumerGroup,
      brokers = kafkaBrokers)(keyDeserializer, valueDeserializer)
      .withProperties(additionalKafkaProperties.asScala.toMap)
    to(consumerSettings)(sink, autoStart)
  }

  private[streams] def to(consumerSettings: ConsumerSettings[Key, Value])(sink: DataHandler[Key, Value], autoStart: Boolean): DataPipeline = {
    implicit val system: ActorSystem = actorSystem
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val pipeline = new ManagedDataPipelineImpl(KafkaStreamManager(kafkaTopic, consumerSettings, replayStrategy, replaySettings, sink.dataHandler), metrics)
    if (autoStart) {
      pipeline.start()
    }
    pipeline
  }
}

object FlowConverter {
  private val log = LoggerFactory.getLogger(getClass)

  def flowFor[K, V, Meta](
    businessLogic: (K, V, Map[String, Array[Byte]]) => Future[Any],
    partitionBy: (K, V, Map[String, Array[Byte]]) => String,
    parallelism: Int)(implicit ec: ExecutionContext): Flow[EventPlusStreamMeta[K, V, Meta], Meta, NotUsed] = {

    Flow.fromGraph(
      GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._
        def toPartition: EventPlusStreamMeta[K, V, Meta] => Int = { t =>
          math.abs(MurmurHash3.stringHash(partitionBy(t.messageKey, t.messageBody, t.headers)) % parallelism)
        }
        val partition = builder.add(Partition[EventPlusStreamMeta[K, V, Meta]](parallelism, toPartition))
        val merge = builder.add(Merge[Meta](parallelism))
        val flow = Flow[EventPlusStreamMeta[K, V, Meta]].mapAsync(1) { evtPlusMeta =>
          businessLogic(evtPlusMeta.messageKey, evtPlusMeta.messageBody, evtPlusMeta.headers).recover {
            case e =>
              log.error(s"An exception was thrown by the event handler for message with metadata[${evtPlusMeta.streamMeta}]. " +
                s"The stream will restart and the message will be retried.", e)
              throw e
          }.map(_ => evtPlusMeta.streamMeta)
        }

        for (_ <- 1 to parallelism) { partition ~> flow ~> merge }

        FlowShape[EventPlusStreamMeta[K, V, Meta], Meta](partition.in, merge.out)
      })
  }
}

trait DataHandler[Key, Value] {
  def dataHandler[Meta]: Flow[EventPlusStreamMeta[Key, Value, Meta], Meta, NotUsed]
}
trait DataSink[Key, Value] extends DataHandler[Key, Value] {
  def parallelism: Int = 8
  def handle(key: Key, value: Value, headers: Map[String, Array[Byte]]): Future[Any]
  def partitionBy(key: Key, value: Value, headers: Map[String, Array[Byte]]): String

  override def dataHandler[Meta]: Flow[EventPlusStreamMeta[Key, Value, Meta], Meta, NotUsed] = {
    FlowConverter.flowFor(handle, partitionBy, parallelism)(ExecutionContext.global)
  }
}

// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Flow, GraphDSL, Merge, Partition }
import akka.{ Done, NotUsed }
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory
import surge.akka.streams.kafka.{ KafkaConsumer, KafkaStreamManager }
import surge.scala.core.kafka.KafkaTopic

import scala.concurrent.{ ExecutionContext, Future }
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

  def to(sink: DataHandler[Key, Value], consumerGroup: String): DataPipeline = {
    val consumerSettings = KafkaConsumer.consumerSettings[Key, Value](actorSystem, groupId = consumerGroup,
      brokers = kafkaBrokers)(keyDeserializer, valueDeserializer)
    to(consumerSettings)(sink)
  }

  private[core] def to(consumerSettings: ConsumerSettings[Key, Value])(sink: DataHandler[Key, Value]): DataPipeline = {
    implicit val system: ActorSystem = actorSystem
    implicit val executionContext: ExecutionContext = ExecutionContext.global

    new ManagedDataPipelineImpl(KafkaStreamManager(kafkaTopic, consumerSettings, replayStrategy, replaySettings, sink.dataHandler).start())
  }
}

object FlowConverter {
  private val log = LoggerFactory.getLogger(getClass)

  def flowFor[T](
    businessLogic: T ⇒ Future[Any],
    partitionBy: T ⇒ String,
    parallelism: Int)(implicit ec: ExecutionContext): Flow[T, Done, NotUsed] = {

    Flow.fromGraph(
      GraphDSL.create() { implicit builder ⇒
        import GraphDSL.Implicits._
        def toPartition: T ⇒ Int = { t ⇒ math.abs(MurmurHash3.stringHash(partitionBy(t)) % parallelism) }
        val partition = builder.add(Partition[T](parallelism, toPartition))
        val merge = builder.add(Merge[Done](parallelism))
        val flow = Flow[T].mapAsync(1) { t ⇒
          businessLogic(t).recover {
            case e ⇒
              log.error(s"An exception was thrown by the event handler! The stream will restart and the message will be retried.", e)
              throw e
          }.map(_ ⇒ Done)
        }

        for (_ ← 1 to parallelism) { partition ~> flow.async ~> merge }

        FlowShape[T, Done](partition.in, merge.out)
      })
  }
}

trait DataHandler[Key, Value] {
  def dataHandler: Flow[(Key, Value), Any, NotUsed]
}
trait DataSink[Key, Value] extends DataHandler[Key, Value] {
  def parallelism: Int = 8
  def handle(key: Key, value: Value): Future[Any]
  def partitionBy(kv: (Key, Value)): String

  override def dataHandler: Flow[(Key, Value), Any, NotUsed] = {
    val tupHandler: ((Key, Value)) ⇒ Future[Any] = { tup ⇒ handle(tup._1, tup._2) }
    FlowConverter.flowFor(tupHandler, partitionBy, parallelism)(ExecutionContext.global)
  }
}

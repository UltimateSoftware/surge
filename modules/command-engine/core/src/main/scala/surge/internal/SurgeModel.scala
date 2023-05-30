// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import surge.core.{ KafkaProducerActor, SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.internal.domain.SurgeProcessingModel
import surge.internal.kafka.{ HeadersHelper, ProducerActorContext }
import surge.internal.utils.DiagnosticContextFuturePropagation
import surge.kafka.KafkaTopic
import surge.metrics.{ MetricInfo, Metrics }

import java.util.concurrent.Executors
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.ExecutionContext.global

trait SurgeModel[State, Message, Event] extends ProducerActorContext {
  private val log = LoggerFactory.getLogger(getClass)

  def aggregateReadFormatting: SurgeAggregateReadFormatting[State]
  def aggregateWriteFormatting: SurgeAggregateWriteFormatting[State]
  def eventWriteFormattingOpt: Option[SurgeEventWriteFormatting[Event]]
  def model: SurgeProcessingModel[State, Message, Event]
  val executionContext: ExecutionContext = global

  private val serializationThreadPoolSize: Int = ConfigFactory.load().getInt("surge.serialization.thread-pool-size")
  private val serializationExecutionContext: ExecutionContext = new DiagnosticContextFuturePropagation(
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(serializationThreadPoolSize)))

  private lazy val serializeEventTimer = metrics.timer(Metrics.SURGE_AGGREGATE_EVENT_SERIALIZATION_TIMER.withTags(Map("aggregate" -> aggregateName)))

  private lazy val serializeStateTimer = metrics.timer(Metrics.SURGE_AGGREGATE_AGGREGATE_STATE_SERIALIZATION_TIMER.withTags(Map("aggregate" -> aggregateName)))

  def serializeEvents(events: Seq[(Event, Option[KafkaTopic])]): Future[Seq[KafkaProducerActor.MessageToPublish]] = Future {
    val eventWriteFormatting = eventWriteFormattingOpt.getOrElse {
      throw new IllegalStateException("businessLogic.eventWriteFormattingOpt must not be None")
    }
    events.flatMap { case (event, topicOpt) =>
      topicOpt.map { topic =>
        val serializedMessage = serializeEventTimer.time(eventWriteFormatting.writeEvent(event))
        log.trace(s"Publishing event for {} {}", Seq(aggregateName, serializedMessage.key): _*)
        KafkaProducerActor.MessageToPublish(
          // Using null here since we need to add the headers but we don't want to explicitly assign the partition
          new ProducerRecord(
            topic.name,
            null, // scalastyle:ignore null
            serializedMessage.key,
            serializedMessage.value,
            HeadersHelper.createHeaders(serializedMessage.headers)))
      }
    }
  }(serializationExecutionContext)

  def serializeState(aggregateId: String, stateValueOpt: Option[State], assignedPartition: TopicPartition): Future[KafkaProducerActor.MessageToPublish] =
    Future {
      val serializedStateOpt = stateValueOpt.map { value =>
        serializeStateTimer.time(aggregateWriteFormatting.writeState(value))
      }
      val stateValue = serializedStateOpt.map(_.value).orNull
      val stateHeaders = serializedStateOpt.map(ser => HeadersHelper.createHeaders(ser.headers)).orNull
      KafkaProducerActor.MessageToPublish(new ProducerRecord(kafka.stateTopic.name, assignedPartition.partition(), aggregateId, stateValue, stateHeaders))
    }(serializationExecutionContext)
}

// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import surge.core.{ KafkaProducerActor, SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.internal.domain.SurgeProcessingModel
import surge.internal.kafka.{ HeadersHelper, ProducerActorContext }
import surge.internal.utils.DiagnosticContextFuturePropagation
import surge.metrics.MetricInfo

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

  private lazy val serializeEventTimer = metrics.timer(
    MetricInfo(
      name = s"surge.aggregate.event-serialization-timer",
      description = "Average time taken in milliseconds to serialize an individual event to bytes before persisting to Kafka",
      tags = Map("aggregate" -> aggregateName)))

  private lazy val serializeStateTimer = metrics.timer(
    MetricInfo(
      name = s"surge.aggregate.aggregate-state-serialization-timer",
      description = "Average time taken in milliseconds to serialize a new aggregate state to bytes before persisting to Kafka",
      tags = Map("aggregate" -> aggregateName)))

  def serializeEvents(events: Seq[Event]): Future[Seq[KafkaProducerActor.MessageToPublish]] = Future {
    val eventWriteFormatting = eventWriteFormattingOpt.getOrElse {
      throw new IllegalStateException("businessLogic.eventWriteFormattingOpt must not be None")
    }
    events.map { event =>
      val serializedMessage = serializeEventTimer.time(eventWriteFormatting.writeEvent(event))
      log.trace(s"Publishing event for {} {}", Seq(aggregateName, serializedMessage.key): _*)
      KafkaProducerActor.MessageToPublish(
        key = serializedMessage.key,
        value = serializedMessage.value,
        headers = HeadersHelper.createHeaders(serializedMessage.headers))
    }

  }(serializationExecutionContext)

  def serializeState(aggregateId: String, stateValueOpt: Option[State]): Future[KafkaProducerActor.MessageToPublish] = Future {
    val serializedStateOpt = stateValueOpt.map { value =>
      serializeStateTimer.time(aggregateWriteFormatting.writeState(value))
    }
    val stateValue = serializedStateOpt.map(_.value).orNull
    val stateHeaders = serializedStateOpt.map(ser => HeadersHelper.createHeaders(ser.headers)).orNull
    KafkaProducerActor.MessageToPublish(aggregateId, stateValue, stateHeaders)
  }(serializationExecutionContext)
}

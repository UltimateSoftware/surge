// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import akka.actor.Actor.Receive
import akka.actor.{ ActorContext, ActorRef, NoSerializationVerificationNeeded }
import akka.pattern._
import com.typesafe.config.ConfigFactory
import io.opentelemetry.api.trace.Span
import org.slf4j.LoggerFactory
import surge.core.KafkaProducerActor
import surge.exceptions.KafkaPublishTimeoutException
import surge.internal.utils.DiagnosticContextFuturePropagation
import surge.metrics.Timer

import java.time.Instant
import scala.concurrent.{ ExecutionContext, Future }

trait KTablePersistenceMetrics {
  def eventPublishTimer: Timer
}

trait KTablePersistenceSupport[Agg, Event] {
  protected def aggregateId: String
  protected def aggregateName: String
  protected def kafkaProducerActor: KafkaProducerActor
  protected def context: ActorContext
  protected def ktablePersistenceMetrics: KTablePersistenceMetrics
  protected def self: ActorRef
  protected def sender(): ActorRef

  protected type ActorState
  protected def receiveWhilePersistingEvents(state: ActorState): Receive
  protected def onPersistenceSuccess(newState: ActorState): Unit
  protected def onPersistenceFailure(state: ActorState, cause: Throwable): Unit

  private val log = LoggerFactory.getLogger(getClass)
  protected val maxProducerFailureRetries: Int

  private sealed trait Internal extends NoSerializationVerificationNeeded
  private case class PersistenceSuccess(newState: ActorState, startTime: Instant) extends Internal
  private case class PersistenceFailure(
      newState: ActorState,
      reason: Throwable,
      numberOfFailures: Int,
      serializedEvents: Seq[KafkaProducerActor.MessageToPublish],
      serializedState: KafkaProducerActor.MessageToPublish,
      startTime: Instant)
      extends Internal
  private case class EventPublishTimedOut(reason: Throwable, startTime: Instant) extends Internal

  private def handleInternal(state: ActorState): Receive = {
    case msg: PersistenceSuccess   => handle(state, msg)
    case msg: PersistenceFailure   => handleFailedToPersist(state, msg)
    case msg: EventPublishTimedOut => handlePersistenceTimedOut(state, msg)
  }
  protected def persistingEvents(state: ActorState): Receive = handleInternal(state).orElse(receiveWhilePersistingEvents(state))

  protected def doPublish(
      state: ActorState,
      serializedEvents: Seq[KafkaProducerActor.MessageToPublish],
      serializedState: KafkaProducerActor.MessageToPublish,
      currentFailureCount: Int = 0,
      startTime: Instant,
      didStateChange: Boolean)(implicit ec: ExecutionContext): Future[Any] = {
    log.trace("Publishing messages for {}", aggregateId)
    if (serializedEvents.isEmpty && !didStateChange) {
      Future.successful(PersistenceSuccess(state, startTime))
    } else {
      kafkaProducerActor
        .publish(aggregateId = aggregateId, state = serializedState, events = serializedEvents)
        .map {
          case KafkaProducerActor.PublishSuccess    => PersistenceSuccess(state, startTime)
          case KafkaProducerActor.PublishFailure(t) => PersistenceFailure(state, t, currentFailureCount + 1, serializedEvents, serializedState, startTime)
        }
        .recover { case t =>
          EventPublishTimedOut(t, startTime)
        }
    }
  }

  // TODO can we handle this more gracefully? If we're unsure something published or not from the timeout the safest thing to do for now
  //  is to crash the actor and force reinitialization
  private def handlePersistenceTimedOut(state: ActorState, msg: EventPublishTimedOut): Unit = {
    ktablePersistenceMetrics.eventPublishTimer.recordTime(publishTimeInMillis(msg.startTime))
    onPersistenceFailure(state, KafkaPublishTimeoutException(aggregateId, msg.reason))
  }

  private def handleFailedToPersist(state: ActorState, eventsFailedToPersist: PersistenceFailure): Unit = {
    implicit val ec: ExecutionContext = new DiagnosticContextFuturePropagation(context.dispatcher)

    if (eventsFailedToPersist.numberOfFailures > maxProducerFailureRetries) {
      ktablePersistenceMetrics.eventPublishTimer.recordTime(publishTimeInMillis(eventsFailedToPersist.startTime))
      onPersistenceFailure(state, eventsFailedToPersist.reason)
    } else {
      log.warn(
        s"Failed to publish to Kafka after try #${eventsFailedToPersist.numberOfFailures}, retrying for $aggregateName $aggregateId",
        eventsFailedToPersist.reason)
      doPublish(
        eventsFailedToPersist.newState,
        eventsFailedToPersist.serializedEvents,
        eventsFailedToPersist.serializedState,
        eventsFailedToPersist.numberOfFailures,
        eventsFailedToPersist.startTime,
        didStateChange = true).pipeTo(self)(sender())
    }
  }

  private def publishTimeInMillis(startTime: Instant): Long = {
    Instant.now.toEpochMilli - startTime.toEpochMilli
  }

  private def handle(state: ActorState, msg: PersistenceSuccess): Unit = {
    ktablePersistenceMetrics.eventPublishTimer.recordTime(publishTimeInMillis(msg.startTime))
    onPersistenceSuccess(msg.newState)
  }
}

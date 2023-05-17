// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import akka.actor.Actor.Receive
import akka.actor.{ ActorContext, ActorRef, NoSerializationVerificationNeeded }
import akka.pattern._
import org.slf4j.LoggerFactory
import surge.core.KafkaProducerActor
import surge.core.KafkaProducerActor.RetryAwareException
import surge.exceptions.KafkaPublishTimeoutException
import surge.internal.domain.SurgeContextImpl
import surge.metrics.Timer

import java.time.Instant
import java.util.UUID
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
  protected def onPersistenceSuccess(newState: ActorState, surgeContext: SurgeContextImpl[Agg, Event]): Unit
  protected def onPersistenceFailure(state: ActorState, cause: Throwable): Unit

  private val log = LoggerFactory.getLogger(getClass)
  protected val maxProducerFailureRetries: Int

  private sealed trait Internal extends NoSerializationVerificationNeeded
  private case class PersistenceSuccess(persistenceRequestId: UUID, newState: ActorState, startTime: Instant, surgeContext: SurgeContextImpl[Agg, Event])
      extends Internal
  private case class PersistenceFailure(
      persistenceRequestId: UUID,
      newState: ActorState,
      context: SurgeContextImpl[Agg, Event],
      reason: Throwable,
      numberOfFailures: Int,
      serializedMessages: Seq[KafkaProducerActor.MessageToPublish],
      startTime: Instant)
      extends Internal
  private case class EventPublishTimedOut(
      trackingId: UUID,
      aggregateId: String,
      context: SurgeContextImpl[Agg, Event],
      messages: Seq[KafkaProducerActor.MessageToPublish],
      reason: Throwable,
      startTime: Instant,
      newState: ActorState,
      numberOfFailures: Int = 0,
      retry: Boolean = false)
      extends Internal

  private def handleInternal(state: ActorState)(implicit ec: ExecutionContext): Receive = {
    case msg: PersistenceSuccess   => handle(state, msg)
    case msg: PersistenceFailure   => handleFailedToPersist(state, msg)
    case msg: EventPublishTimedOut => handlePersistenceTimedOut(state, msg)
  }
  protected def persistingEvents(state: ActorState)(implicit ec: ExecutionContext): Receive = handleInternal(state).orElse(receiveWhilePersistingEvents(state))

  protected def doPublish(
      state: ActorState,
      context: SurgeContextImpl[Agg, Event],
      serializedMessages: Seq[KafkaProducerActor.MessageToPublish],
      currentFailureCount: Int = 0,
      startTime: Instant,
      shouldPublish: Boolean,
      trackingId: UUID = UUID.randomUUID())(implicit ec: ExecutionContext): Future[Any] = {
    log.trace("Publishing messages for {}", aggregateId)
    if (!shouldPublish) {
      Future.successful(PersistenceSuccess(trackingId, state, startTime, context))
    } else {
      val futureResult = kafkaProducerActor.publish(aggregateId = aggregateId, messages = serializedMessages, requestId = trackingId)
      futureResult
        .map {
          case KafkaProducerActor.PublishSuccess(publishRequestId) => PersistenceSuccess(publishRequestId, state, startTime, context)
          case KafkaProducerActor.PublishFailure(publishRequestId, t, _) =>
            PersistenceFailure(publishRequestId, state, context, t, currentFailureCount + 1, serializedMessages, startTime)
        }
        .recover { case t =>
          log.error("Failed to publish messages", t)

          t match {
            case exception: RetryAwareException =>
              EventPublishTimedOut(
                trackingId = trackingId,
                aggregateId = aggregateId,
                context = context,
                messages = serializedMessages,
                reason = t,
                startTime = startTime,
                retry = exception.retry,
                newState = state,
                numberOfFailures = currentFailureCount + 1)
            case _ =>
              EventPublishTimedOut(
                trackingId = trackingId,
                aggregateId = aggregateId,
                context = context,
                messages = serializedMessages,
                reason = t,
                startTime = startTime,
                newState = state,
                numberOfFailures = currentFailureCount + 1)

          }
        }
    }
  }

  private def handlePersistenceTimedOut(state: ActorState, msg: EventPublishTimedOut): Unit = {
    implicit val ec: ExecutionContext = context.dispatcher
    ktablePersistenceMetrics.eventPublishTimer.recordTime(publishTimeInMillis(msg.startTime))

    if (msg.retry && msg.numberOfFailures < maxProducerFailureRetries) {
      doPublish(
        msg.newState,
        msg.context,
        msg.messages,
        currentFailureCount = msg.numberOfFailures,
        Instant.now(),
        shouldPublish = true,
        trackingId = msg.trackingId).pipeTo(self)
    } else {
      onPersistenceFailure(state, KafkaPublishTimeoutException(aggregateId, msg.reason))
    }
  }

  private def handleFailedToPersist(state: ActorState, eventsFailedToPersist: PersistenceFailure)(implicit ec: ExecutionContext): Unit = {

    if (eventsFailedToPersist.numberOfFailures > maxProducerFailureRetries) {
      ktablePersistenceMetrics.eventPublishTimer.recordTime(publishTimeInMillis(eventsFailedToPersist.startTime))
      onPersistenceFailure(state, eventsFailedToPersist.reason)
    } else {
      log.warn(
        s"Failed to publish to Kafka after try #${eventsFailedToPersist.numberOfFailures}, retrying for $aggregateName $aggregateId",
        eventsFailedToPersist.reason)
      doPublish(
        eventsFailedToPersist.newState,
        eventsFailedToPersist.context,
        eventsFailedToPersist.serializedMessages,
        eventsFailedToPersist.numberOfFailures,
        eventsFailedToPersist.startTime,
        shouldPublish = true).pipeTo(self)(sender())
    }
  }

  private def publishTimeInMillis(startTime: Instant): Long = {
    Instant.now.toEpochMilli - startTime.toEpochMilli
  }

  private def handle(state: ActorState, msg: PersistenceSuccess): Unit = {
    ktablePersistenceMetrics.eventPublishTimer.recordTime(publishTimeInMillis(msg.startTime))
    onPersistenceSuccess(msg.newState, msg.surgeContext)
  }
}

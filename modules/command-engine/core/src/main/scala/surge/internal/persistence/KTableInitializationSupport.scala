// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import akka.actor.ActorContext
import org.slf4j.LoggerFactory
import surge.core.KafkaProducerActor
import surge.exceptions.{ AggregateInitializationException, AggregateStateNotCurrentInKTableException }
import surge.internal.config.RetryConfig
import surge.kafka.streams.AggregateStateStoreKafkaStreams
import surge.metrics.Timer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait KTableInitializationMetrics {
  def stateInitializationTimer: Timer
  def aggregateDeserializationTimer: Timer
}

trait KTableInitializationSupport[Model] {
  private val log = LoggerFactory.getLogger(getClass)
  private case class InitializeWithState(stateOpt: Option[Model])

  def context: ActorContext
  def aggregateId: String
  def aggregateName: String
  def initializationMetrics: KTableInitializationMetrics
  def kafkaProducerActor: KafkaProducerActor
  def kafkaStreamsCommand: AggregateStateStoreKafkaStreams[_]
  def deserializeState(bytes: Array[Byte]): Option[Model]

  def onInitializationFailed(cause: Throwable): Unit
  def onInitializationSuccess(model: Option[Model]): Unit

  protected def initializeState(initializationAttempts: Int, retryCause: Option[Throwable])(implicit ec: ExecutionContext): Unit = {
    if (initializationAttempts > RetryConfig.AggregateActor.maxInitializationAttempts) {
      val reasonForFailure = retryCause.getOrElse(new RuntimeException(s"Aggregate $aggregateId could not be initialized"))
      onInitializationFailed(reasonForFailure)
    } else {
      kafkaProducerActor
        .isAggregateStateCurrent(aggregateId)
        .map { isStateCurrent =>
          if (isStateCurrent) {
            fetchState(initializationAttempts)(ec, timeout = FiniteDuration(30, "seconds"))
          } else {
            val retryIn = RetryConfig.AggregateActor.initializeStateInterval
            log.warn("State for {} is not up to date in Kafka streams, retrying initialization in {}", Seq(aggregateId, retryIn): _*)
            val failureReason = AggregateStateNotCurrentInKTableException(aggregateId, kafkaProducerActor.assignedPartition)
            context.system.scheduler.scheduleOnce(retryIn)(initializeState(initializationAttempts + 1, Some(failureReason)))
          }
        }
        .recover { case _ =>
          val retryIn = RetryConfig.AggregateActor.fetchStateRetryInterval
          val failureReason = AggregateStateNotCurrentInKTableException(aggregateId, kafkaProducerActor.assignedPartition)
          context.system.scheduler.scheduleOnce(retryIn)(initializeState(initializationAttempts + 1, Some(failureReason)))
        }
    }
  }

  private def fetchState(initializationAttempts: Int)(implicit ec: ExecutionContext, timeout: Duration): Unit = {
    val fetchedStateFut = initializationMetrics.stateInitializationTimer.time {
      kafkaStreamsCommand.getAggregateBytes(aggregateId.toString)
    }
    fetchedStateFut
      .map { state =>
        log.trace("Fetched state from KTable for {}", aggregateId)
        val stateOpt = state.flatMap { bodyBytes =>
          initializationMetrics.aggregateDeserializationTimer.time(deserializeState(bodyBytes))
        }

        onInitializationSuccess(stateOpt)
      }
      .recover { case exception =>
        val retryIn = RetryConfig.AggregateActor.fetchStateRetryInterval
        log.error(s"Failed to initialize $aggregateName actor for aggregate $aggregateId, retrying in $retryIn", exception)
        val failureReason = AggregateInitializationException(aggregateId, exception)
        context.system.scheduler.scheduleOnce(retryIn)(initializeState(initializationAttempts + 1, Some(failureReason)))
      }
  }
}

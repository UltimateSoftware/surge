// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout
import com.ultimatesoftware.config.TimeoutConfig
import com.ultimatesoftware.exceptions.{ SurgeTimeoutException, SurgeUnexpectedException }
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.{ ExecutionContext, Future }

/**
 * Generic reference to an aggregate that handles proxying messages to the actual aggregate
 * actor responsible for a particular aggregate id.  A single reference represents a proxy for
 * a single business logic aggregate actor.  Any commands you send to an instance of an AggregateRef
 * will be forwarded to the same business logic aggregate actor responsible for the same aggregateId
 * as the AggregateRef is responsible for.
 *
 * @tparam Agg The type of the business logic aggregate being proxied to
 * @tparam Cmd The command type that the business logic aggregate handles
 * @tparam Event The event type that the business logic aggregate generates and can handle to update state
 */
trait AggregateRefTrait[Agg, Cmd, Event] {

  val aggregateId: String
  val region: ActorRef

  private val askTimeoutDuration = TimeoutConfig.AggregateActor.askTimeout
  private implicit val timeout: Timeout = Timeout(askTimeoutDuration)

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private def interpretActorResponse: Any ⇒ Either[Throwable, Option[Agg]] = {
    case success: GenericAggregateActor.CommandSuccess[Agg] ⇒ Right(success.aggregateState)
    case failure: GenericAggregateActor.CommandFailure      ⇒ Left(new DomainValidationError(failure.validationError))
    case error: GenericAggregateActor.CommandError ⇒
      Left(error.exception)
    case other ⇒
      val errorMsg = s"Unable to interpret response from aggregate - this should not happen: $other"
      log.error(errorMsg)
      Left(SurgeUnexpectedException(new IllegalStateException(errorMsg)))
  }

  /**
   * Asynchronously fetch the current state of the aggregate that this reference is proxying to.
   * @return A future of either None (the aggregate has no state) or some aggregate state for the
   *         aggregate with the aggregate id of this reference
   */
  protected def queryState(implicit ec: ExecutionContext): Future[Option[Agg]] = {
    (region ? GenericAggregateActor.GetState(aggregateId)).mapTo[GenericAggregateActor.StateResponse[Agg]].map(_.aggregateState)
  }

  /**
   * Asynchronously send a command envelope to the aggregate business logic actor this reference is
   * talking to. Retries a given number of times if sending the command envelope to the business logic
   * actor fails.
   *
   * @param envelope The command envelope to send to this aggregate actor
   * @param retriesRemaining Number of retry attempts remaining, defaults to 0 for no retries.
   * @param ec Implicit execution context to use for transforming the raw actor response into a
   *           better typed response.
   * @return A future of either validation errors from the business logic aggregate or the updated
   *         state of the business logic aggregate after handling the command and applying any events
   *         that resulted from the command.
   */
  protected def askWithRetries(
    envelope: GenericAggregateActor.CommandEnvelope[Cmd],
    retriesRemaining: Int = 0)(implicit ec: ExecutionContext): Future[Either[Throwable, Option[Agg]]] = {
    (region ? envelope).map(interpretActorResponse).recoverWith {
      case _: Throwable if retriesRemaining > 0 ⇒
        log.warn("Ask timed out to aggregate actor region, retrying request...")
        askWithRetries(envelope, retriesRemaining - 1)
      case _: AskTimeoutException ⇒
        val msg = s"Ask timed out after $askTimeoutDuration to aggregate actor with id ${envelope.aggregateId} executing command ${envelope.command.getClass.getName}. " +
          s"This is typically a result of other parts of the engine performing incorrectly or hitting exceptions"
        Future.successful[Either[Throwable, Option[Agg]]](Left(SurgeTimeoutException(msg)))
      case e: Throwable ⇒
        Future.successful[Either[Throwable, Option[Agg]]](Left(SurgeUnexpectedException(e)))
    }
  }

  protected def applyEventsWithRetries(
    envelope: GenericAggregateActor.ApplyEventEnvelope[Event],
    retriesRemaining: Int = 0)(implicit ec: ExecutionContext): Future[Option[Agg]] = {
    (region ? envelope).map {
      case success: GenericAggregateActor.CommandSuccess[Agg] ⇒ success.aggregateState
    }.recoverWith {
      case e ⇒
        if (retriesRemaining > 0) {
          log.warn("Ask timed out to aggregate actor region, retrying request...")
          applyEventsWithRetries(envelope, retriesRemaining - 1)
        } else {
          Future.failed[Option[Agg]](e)
        }
    }
  }
}

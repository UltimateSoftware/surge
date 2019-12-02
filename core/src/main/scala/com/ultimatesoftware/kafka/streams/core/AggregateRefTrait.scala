// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.util.concurrent.TimeUnit

import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.scala.core.validations.ValidationError
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

/**
 * Generic reference to an aggregate that handles proxying messages to the actual aggregate
 * actor responsible for a particular aggregate id.  A single reference represents a proxy for
 * a single business logic aggregate actor.  Any commands you send to an instance of an AggregateRef
 * will be forwarded to the same business logic aggregate actor responsible for the same aggregateId
 * as the AggregateRef is responsible for.
 *
 * @tparam AggIdType The type of the aggregate id for the underlying business logic aggregate
 * @tparam Agg The type of the business logic aggregate being proxied to
 * @tparam Cmd The command type that the business logic aggregate handles
 * @tparam CmdMeta Type of command metadata that is sent along with each command
 */
trait AggregateRefTrait[AggIdType, Agg, Cmd, CmdMeta] {

  val aggregateId: AggIdType
  val region: ActorRef
  val system: ActorSystem

  private val config = ConfigFactory.load()
  private val askTimeoutDuration = config.getDuration("ulti.aggregate-actor.ask-timeout", TimeUnit.SECONDS).seconds
  private implicit val timeout: Timeout = Timeout(askTimeoutDuration)

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private def interpretActorResponse: Any ⇒ Either[Seq[ValidationError], Option[Agg]] = {
    case success: GenericAggregateActor.CommandSuccess[Agg] ⇒ Right(success.aggregateState)
    case failure: GenericAggregateActor.CommandFailure      ⇒ Left(failure.validationError)
    case other ⇒
      log.error(s"Unable to interpret response from aggregate - this should not happen: $other")
      Right(None)
  }

  /**
   * Asynchronously fetch the current state of the aggregate that this reference is proxying to.
   * @return A future of either None (the aggregate has no state) or some aggregate state for the
   *         aggregate with the aggregate id of this reference
   */
  protected def queryState: Future[Option[Agg]] = {
    (region ? GenericAggregateActor.GetState(aggregateId)).mapTo[Option[Agg]]
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
    envelope: GenericAggregateActor.CommandEnvelope[AggIdType, Cmd, CmdMeta],
    retriesRemaining: Int = 0)(implicit ec: ExecutionContext): Future[Either[Seq[ValidationError], Option[Agg]]] = {
    (region ? envelope).map(interpretActorResponse).recoverWith {
      case e ⇒
        if (retriesRemaining > 0) {
          log.warn("Ask timed out to aggregate actor region, retrying request...")
          askWithRetries(envelope, retriesRemaining - 1)
        } else {
          Future.failed[Either[Seq[ValidationError], Option[Agg]]](e)
        }
    }
  }

}

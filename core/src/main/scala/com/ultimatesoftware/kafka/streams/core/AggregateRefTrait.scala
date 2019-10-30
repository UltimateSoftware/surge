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

  protected def queryState: Future[Option[Agg]] = {
    (region ? GenericAggregateActor.GetState(aggregateId)).mapTo[Option[Agg]]
  }

  protected def askWithRetries(
    envelope: GenericAggregateActor.CommandEnvelope[AggIdType, Cmd, CmdMeta],
    retriesRemaining: Int = 1)(implicit ec: ExecutionContext): Future[Either[Seq[ValidationError], Option[Agg]]] = {
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

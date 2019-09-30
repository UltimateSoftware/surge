// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern.{ ask ⇒ akkaAsk }
import akka.util.Timeout
import com.ultimatesoftware.kafka.streams.core.GenericAggregateActor
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.JsValue

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

final class AggregateRef[AggIdType, Cmd, CmdMeta](val aggregateId: AggIdType, region: ActorRef, system: ActorSystem) {
  implicit val timeout: Timeout = Timeout(25.seconds)

  private val log: Logger = LoggerFactory.getLogger(getClass)

  def ask(commandProps: CmdMeta, command: Cmd): Future[JsValue] = {
    val envelope = GenericAggregateActor.CommandEnvelope[AggIdType, Cmd, CmdMeta](aggregateId, commandProps, command)
    askWithRetries(envelope, 0)
  }

  private def askWithRetries(envelope: GenericAggregateActor.CommandEnvelope[AggIdType, Cmd, CmdMeta], retriesRemaining: Int = 1): Future[JsValue] = {
    (region ? envelope).mapTo[JsValue].recoverWith {
      case e ⇒
        if (retriesRemaining > 0) {
          log.warn("Ask timed out to aggregate actor region, retrying request...")
          askWithRetries(envelope, retriesRemaining - 1)
        } else {
          Future.failed[JsValue](e)
        }
    }(ExecutionContext.global)
  }
}

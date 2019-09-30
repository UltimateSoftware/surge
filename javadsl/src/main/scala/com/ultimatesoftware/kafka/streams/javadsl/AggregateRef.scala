// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.concurrent.CompletionStage

import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern.{ ask ⇒ akkaAsk }
import akka.util.Timeout
import com.ultimatesoftware.kafka.streams.core.GenericAggregateActor
import play.api.libs.json.JsValue

import scala.compat.java8.FutureConverters
import scala.concurrent.duration._

final class AggregateRef[AggIdType, Cmd, CmdMeta](val aggregateId: AggIdType, region: ActorRef, system: ActorSystem) {
  private implicit val timeout: Timeout = Timeout(15.seconds)

  def ask(commandProps: CmdMeta, command: Cmd): CompletionStage[JsValue] = {
    val envelope = GenericAggregateActor.CommandEnvelope[AggIdType, Cmd, CmdMeta](aggregateId, commandProps, command)
    val scalaFuture = (region ? envelope).mapTo[JsValue]
    FutureConverters.toJava(scalaFuture)
  }
}

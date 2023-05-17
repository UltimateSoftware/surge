// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import surge.core
import surge.core.Ack

import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters

class ControllableAdapter(controllable: core.Controllable) {

  def start(): CompletionStage[Ack] = FutureConverters.toJava(controllable.start())

  def restart(): CompletionStage[Ack] = FutureConverters.toJava(controllable.restart())

  def stop(): CompletionStage[Ack] = FutureConverters.toJava(controllable.stop())

  def shutdown(): CompletionStage[Ack] = FutureConverters.toJava(controllable.shutdown())
}

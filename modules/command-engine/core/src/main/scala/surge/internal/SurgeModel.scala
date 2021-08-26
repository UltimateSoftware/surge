// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal

import surge.core.{ SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.internal.domain.AggregateProcessingModel
import surge.internal.kafka.ProducerActorContext

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global

trait SurgeModel[State, Message, +Rejection, Event, Response] extends ProducerActorContext {
  def aggregateReadFormatting: SurgeAggregateReadFormatting[State]
  def aggregateWriteFormatting: SurgeAggregateWriteFormatting[State]
  def eventWriteFormattingOpt: Option[SurgeEventWriteFormatting[Event]]
  def model: AggregateProcessingModel[State, Message, Rejection, Event, Response]
  val executionContext: ExecutionContext = global
}

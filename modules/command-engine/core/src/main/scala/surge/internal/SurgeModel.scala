// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal

import surge.core.{ SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting }
import surge.internal.domain.AggregateProcessingModel
import surge.internal.kafka.ProducerActorContext
import surge.internal.utils.DiagnosticContextFuturePropagation

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global

trait SurgeModel[S, M, +R, E] extends ProducerActorContext {
  def aggregateReadFormatting: SurgeAggregateReadFormatting[S]
  def aggregateWriteFormatting: SurgeAggregateWriteFormatting[S]
  def eventWriteFormattingOpt: Option[SurgeEventWriteFormatting[E]]
  def model: AggregateProcessingModel[S, M, R, E]
  val executionContext: ExecutionContext = new DiagnosticContextFuturePropagation(global)
}

// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.common

import akka.actor.ActorRef
import io.opentelemetry.api.trace.Tracer
import surge.internal.persistence.{ AggregateRefTrait, PersistentActor }

import java.util.Optional
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext

trait AggregateRefBaseTrait[AggId, Agg, Cmd, Event] extends AggregateRefTrait[AggId, Agg, Cmd, Event] {

  val aggregateId: AggId
  protected val region: ActorRef
  protected val tracer: Tracer

  private implicit val ec: ExecutionContext = ExecutionContext.global

  def getState: CompletionStage[Optional[Agg]] = {
    FutureConverters.toJava(queryState.map(_.asJava))
  }

  def applyEvents(events: Seq[Event]): CompletionStage[ApplyEventResult[Agg]] = {
    val envelope = PersistentActor.ApplyEvents[Event](aggregateId.toString, events)
    val result = applyEvents(envelope).map(aggOpt => ApplyEventSuccess[Agg](aggOpt.asJava)).recover { case e =>
      ApplyEventFailure[Agg](e)
    }
    FutureConverters.toJava(result)
  }
}

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

trait AggregateRefBaseTrait[AggId, Agg, Cmd, Event, Response] extends AggregateRefTrait[AggId, Agg, Cmd, Event, Response] {

  val aggregateId: AggId
  protected val region: ActorRef
  protected val tracer: Tracer

  private implicit val ec: ExecutionContext = ExecutionContext.global

  def getState: CompletionStage[Optional[Agg]] = {
    FutureConverters.toJava(queryState.map(_.asJava))
  }

  def applyEvent(event: Event): CompletionStage[ApplyEventResult[Response]] = {
    val envelope = PersistentActor.ApplyEvent[Event](aggregateId.toString, event)
    val result = doApplyEvents(envelope).map(aggOpt => ApplyEventSuccess[Response](aggOpt.asJava)).recover { case e =>
      ApplyEventFailure[Response](e)
    }
    FutureConverters.toJava(result)
  }
}

// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import akka.actor.ActorRef
import akka.actor.Status.Success
import akka.pattern._
import akka.util.Timeout
import io.opentelemetry.api.trace.{ Span, Tracer }
import org.slf4j.{ Logger, LoggerFactory }
import surge.exceptions.{ SurgeTimeoutException, SurgeUnexpectedException }
import surge.internal.config.TimeoutConfig
import surge.internal.tracing.TracingHelper._
import surge.internal.tracing.{ SpanSupport, TracedMessage }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure

/**
 * Generic reference to an aggregate that handles proxying messages to the actual aggregate actor responsible for a particular aggregate id. A single reference
 * represents a proxy for a single business logic aggregate actor. Any commands you send to an instance of an AggregateRef will be forwarded to the same
 * business logic aggregate actor responsible for the same aggregateId as the AggregateRef is responsible for.
 *
 * @tparam AggId
 *   The type of the aggregate id for the underlying business logic aggregate
 * @tparam Agg
 *   The type of the business logic aggregate being proxied to
 * @tparam Cmd
 *   The command type that the business logic aggregate handles
 * @tparam Event
 *   The event type that the business logic aggregate generates and can handle to update state
 */
private[surge] trait AggregateRefTrait[AggId, Agg, Cmd, Event] extends SpanSupport {

  val aggregateId: AggId
  protected val region: ActorRef
  protected val tracer: Tracer

  private val askTimeoutDuration = TimeoutConfig.AggregateActor.askTimeout
  private implicit val timeout: Timeout = Timeout(askTimeoutDuration)

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private def interpretActorResponse(span: Span): Any => Either[Throwable, Option[Agg]] = {
    case success: PersistentActor.ACKSuccess[Agg] =>
      span.finish()
      Right(success.aggregateState)
    case error: PersistentActor.ACKError =>
      span.error(error.exception)
      span.finish()
      Left(error.exception)
    case other =>
      val errorMsg = s"Unable to interpret response from aggregate - this should not happen: $other"
      log.error(errorMsg)
      Left(SurgeUnexpectedException(new IllegalStateException(errorMsg)))
  }

  /**
   * Asynchronously fetch the current state of the aggregate that this reference is proxying to.
   *
   * @return
   *   A future of either None (the aggregate has no state) or some aggregate state for the aggregate with the aggregate id of this reference
   */
  protected def queryState(implicit ec: ExecutionContext): Future[Option[Agg]] = {
    (region ? PersistentActor.GetState(aggregateId.toString)).mapTo[PersistentActor.StateResponse[Agg]].map(_.aggregateState)
  }

  /**
   * Asynchronously send a command envelope to the aggregate business logic actor this reference is talking to.
   *
   * @param envelope
   *   The command envelope to send to this aggregate actor
   * @param ec
   *   Implicit execution context to use for transforming the raw actor response into a better typed response.
   * @return
   *   A future of the updated state of the business logic aggregate after handling the command and applying any events that resulted from the command.
   */
  protected def sendCommand(envelope: PersistentActor.ProcessMessage[Cmd])(implicit ec: ExecutionContext): Future[Option[Agg]] = {
    val askSpan = createSpan(envelope.message.getClass.getSimpleName).setTag("aggregateId", aggregateId.toString)
    askSpan.log("actor ask", Map("timeout" -> timeout.duration.toString()))
    (region ? TracedMessage(envelope, askSpan)(tracer))
      .map(interpretActorResponse(askSpan))
      .flatMap {
        case Left(exception) => Future.failed(exception)
        case Right(state)    => Future.successful(state)
      }
      .recoverWith { case a: AskTimeoutException =>
        val msg = s"Ask timed out after $askTimeoutDuration to aggregate actor with id ${envelope.aggregateId} executing command " +
          s"${envelope.message.getClass.getName}. This is typically a result of other parts of the engine performing incorrectly or " +
          s"hitting exceptions"
        askSpan.error(a)
        askSpan.finish()
        Future.failed(SurgeTimeoutException(msg))
      }
  }

  protected def applyEvents(envelope: PersistentActor.ApplyEvents[Event])(implicit ec: ExecutionContext): Future[Option[Agg]] = {
    val askSpan = createSpan("send_events_to_aggregate").setTag("aggregateId", aggregateId.toString)
    (region ? TracedMessage(envelope, askSpan)(tracer)).map(interpretActorResponse(askSpan)).flatMap {
      case Left(exception) => Future.failed(exception)
      case Right(state)    => Future.successful(state)
    }
  }
}

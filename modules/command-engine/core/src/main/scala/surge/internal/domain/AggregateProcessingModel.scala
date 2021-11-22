// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.ActorRef
import surge.internal.persistence.PersistentActor.{ ACKRejection, ACKSuccess }

import scala.concurrent.{ ExecutionContext, Future }

trait SurgeSideEffect[State]
private[surge] class Callback[State](val sideEffect: Option[State] => Unit) extends SurgeSideEffect[State]
private[surge] class ReplyEffect[State, Reply](replyTo: ActorRef, replyWithMessage: Option[State] => Reply)
    extends Callback[State](state => replyTo ! replyWithMessage(state))

trait SurgeProcessingModel[State, Message, Event] {
  def handle(ctx: SurgeContext[State, Event], state: Option[State], msg: Message)(implicit ec: ExecutionContext): Future[SurgeContext[State, Event]]

  def applyAsync(ctx: SurgeContext[State, Event], state: Option[State], event: Event): Future[SurgeContext[State, Event]]
}

trait SurgeContext[State, Event] {
  def persistEvent(event: Event): SurgeContext[State, Event]
  def persistEvents(events: Seq[Event]): SurgeContext[State, Event]
  def updateState(state: Option[State]): SurgeContext[State, Event]
  def reply[Reply](replyWithMessage: Option[State] => Option[Reply]): SurgeContext[State, Event]
  def reject[Rejection](rejection: Rejection): SurgeContext[State, Event]
  def nothing: SurgeContext[State, Event]
}

case class SurgeContextImpl[State, Event](
    originalSender: ActorRef,
    sideEffects: Seq[SurgeSideEffect[State]] = Seq.empty,
    state: Option[State] = None,
    isRejected: Boolean = false,
    events: Seq[Event] = Seq.empty)
    extends SurgeContext[State, Event] {
  override def persistEvent(event: Event): SurgeContextImpl[State, Event] = copy(events = events :+ event)
  override def persistEvents(events: Seq[Event]): SurgeContextImpl[State, Event] = copy(events = this.events ++ events)
  override def updateState(state: Option[State]): SurgeContextImpl[State, Event] = copy(state = state)
  override def reply[Reply](replyWithMessage: Option[State] => Option[Reply]): SurgeContextImpl[State, Event] =
    addSideEffect(new ReplyEffect(originalSender, { state => ACKSuccess(replyWithMessage(state)) }))
  override def reject[Rejection](rejection: Rejection): SurgeContextImpl[State, Event] =
    addSideEffect(new ReplyEffect(originalSender, _ => ACKRejection(rejection))).copy(isRejected = true)

  private def addSideEffect(sideEffect: SurgeSideEffect[State]): SurgeContextImpl[State, Event] = copy(sideEffects = this.sideEffects :+ sideEffect)

  override def nothing: SurgeContext[State, Event] = this
}

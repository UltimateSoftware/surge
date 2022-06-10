// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.ActorRef
import org.apache.kafka.clients.producer.ProducerRecord
import surge.internal.persistence.PersistentActor.{ ACKRejection, ACKSuccess }

import scala.concurrent.{ ExecutionContext, Future }

trait SurgeSideEffect[State]
private[surge] class Callback[State](val sideEffect: Option[State] => Unit) extends SurgeSideEffect[State]
private[surge] class ReplyEffect[State, Reply](replyTo: ActorRef, replyWithMessage: Option[State] => Reply)
    extends Callback[State](state => replyTo ! replyWithMessage(state))

trait SurgeProcessingModel[State, Message, Event] {
  def handle(ctx: SurgeContext[State, Event], state: Option[State], msg: Message)(implicit ec: ExecutionContext): Future[SurgeContext[State, Event]]

  // FIXME This should be a singular event and event based models should leverage the new context support for just an "updateState" call
  def applyAsync(ctx: SurgeContext[State, Event], state: Option[State], events: Seq[Event]): Future[SurgeContext[State, Event]]
}

trait SurgeContext[State, Event] {
  def persistEvent(event: Event): SurgeContext[State, Event]
  def persistEvents(events: Seq[Event]): SurgeContext[State, Event]
  def persistRecord(record: ProducerRecord[String, Array[Byte]]): SurgeContext[State, Event]
  def persistRecords(records: Seq[ProducerRecord[String, Array[Byte]]]): SurgeContext[State, Event]
  def updateState(state: Option[State]): SurgeContext[State, Event]
  def reply[Reply](replyWithMessage: Option[State] => Option[Reply]): SurgeContext[State, Event]
  def reject[Rejection](rejection: Rejection): SurgeContext[State, Event]
}

case class SurgeContextImpl[State, Event](
    originalSender: ActorRef,
    state: Option[State],
    sideEffects: Seq[SurgeSideEffect[State]] = Seq.empty,
    isRejected: Boolean = false,
    events: Seq[Event] = Seq.empty,
    records: Seq[ProducerRecord[String, Array[Byte]]] = Seq.empty)
    extends SurgeContext[State, Event] {
  override def persistEvent(event: Event): SurgeContextImpl[State, Event] = copy(events = events :+ event)
  override def persistEvents(events: Seq[Event]): SurgeContextImpl[State, Event] = copy(events = this.events ++ events)

  override def persistRecord(record: ProducerRecord[String, Array[Byte]]): SurgeContextImpl[State, Event] = copy(records = records :+ record)
  override def persistRecords(records: Seq[ProducerRecord[String, Array[Byte]]]): SurgeContextImpl[State, Event] = copy(records = this.records ++ records)

  override def updateState(state: Option[State]): SurgeContextImpl[State, Event] = copy(state = state)
  override def reply[Reply](replyWithMessage: Option[State] => Option[Reply]): SurgeContextImpl[State, Event] =
    addSideEffect(new ReplyEffect(originalSender, { state => ACKSuccess(replyWithMessage(state)) }))
  override def reject[Rejection](rejection: Rejection): SurgeContextImpl[State, Event] =
    addSideEffect(new ReplyEffect(originalSender, _ => ACKRejection(rejection))).copy(isRejected = true)

  private def addSideEffect(sideEffect: SurgeSideEffect[State]): SurgeContextImpl[State, Event] = copy(sideEffects = this.sideEffects :+ sideEffect)
}

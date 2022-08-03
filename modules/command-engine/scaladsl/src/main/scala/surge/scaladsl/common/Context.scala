// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.common

import org.apache.kafka.clients.producer.ProducerRecord
import surge.internal.domain.SurgeContext
import surge.kafka.KafkaTopic

trait Context[State, Event] {
  def persistEvent(event: Event): Context[State, Event]
  def persistEvents(events: Seq[Event]): Context[State, Event]
  def persistToTopic(event: Event, topic: KafkaTopic): Context[State, Event]
  def persistRecord(record: ProducerRecord[String, Array[Byte]]): Context[State, Event]
  def persistRecords(records: Seq[ProducerRecord[String, Array[Byte]]]): Context[State, Event]
  def updateState(state: Option[State]): Context[State, Event]
  def reply[Reply](replyWithMessage: Option[State] => Option[Reply]): Context[State, Event]
  def reject[Rejection](rejection: Rejection): Context[State, Event]

  private[surge] def toCore: SurgeContext[State, Event]
}
object Context {
  def apply[State, Event](core: SurgeContext[State, Event]): Context[State, Event] = ContextImpl(core)
}

case class ContextImpl[State, Event](private val core: SurgeContext[State, Event]) extends Context[State, Event] {
  override def persistEvent(event: Event): Context[State, Event] = copy(core = core.persistEvent(event))
  override def persistEvents(events: Seq[Event]): Context[State, Event] = copy(core = core.persistEvents(events))
  override def persistToTopic(event: Event, topic: KafkaTopic): Context[State, Event] = copy(core = core.persistToTopic(event, topic))
  override def persistRecord(record: ProducerRecord[String, Array[Byte]]): Context[State, Event] = copy(core = core.persistRecord(record))
  override def persistRecords(records: Seq[ProducerRecord[String, Array[Byte]]]): Context[State, Event] = copy(core = core.persistRecords(records))
  override def updateState(state: Option[State]): Context[State, Event] = copy(core = core.updateState(state))
  override def reply[Reply](replyWithMessage: Option[State] => Option[Reply]): Context[State, Event] = copy(core = core.reply(replyWithMessage))
  override def reject[Rejection](rejection: Rejection): Context[State, Event] = copy(core = core.reject(rejection))
  override private[surge] def toCore: SurgeContext[State, Event] = core
}

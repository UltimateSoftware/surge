// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.common

import surge.internal.domain.SurgeContext

import java.util.Optional

import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

trait ReplyExtractor[State, Reply] {
  def extractReply(state: Optional[State]): Optional[Reply]
}

trait Context[State, Event] {
  def persistEvent(event: Event): Context[State, Event]
  def persistEvents(events: java.util.List[Event]): Context[State, Event]
  def updateState(state: Optional[State]): Context[State, Event]
  def reply[Reply](replyExtractor: ReplyExtractor[State, Reply]): Context[State, Event]
  def reject[Rejection](rejection: Rejection): Context[State, Event]

  private[surge] def toCore: SurgeContext[State, Event]
}
object Context {
  def apply[State, Event](core: SurgeContext[State, Event]): Context[State, Event] = ContextImpl(core)
}

case class ContextImpl[State, Event](private val core: SurgeContext[State, Event]) extends Context[State, Event] {
  override def persistEvent(event: Event): Context[State, Event] = copy(core = core.persistEvent(event))
  override def persistEvents(events: java.util.List[Event]): Context[State, Event] = copy(core = core.persistEvents(events.asScala.toSeq))
  override def updateState(state: Optional[State]): Context[State, Event] = copy(core = core.updateState(state.asScala))
  override def reply[Reply](replyExtractor: ReplyExtractor[State, Reply]): Context[State, Event] = {
    val replyWithMessage = { state: Option[State] => replyExtractor.extractReply(state.asJava).asScala }
    copy(core = core.reply(replyWithMessage))
  }
  override def reject[Rejection](rejection: Rejection): Context[State, Event] = copy(core = core.reject(rejection))
  override private[surge] def toCore: SurgeContext[State, Event] = core
}

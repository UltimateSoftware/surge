// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl.test

class EventBus[Event] {
  private var bus: Seq[Event] = Seq.empty
  def send(event: Event): Unit = {
    bus = bus :+ event
  }
  def send(events: Seq[Event]): Unit = {
    events.foreach(evt ⇒ send(evt))
  }

  def consumeOne(): Option[Event] = {
    bus match {
      case head +: tail ⇒
        bus = tail
        Some(head)
      case _ ⇒
        None
    }
  }

  def clear(): Unit = {
    bus = Seq.empty
  }
}

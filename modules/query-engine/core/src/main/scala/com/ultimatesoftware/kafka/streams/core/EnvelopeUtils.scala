// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.scala.core.messaging.EventProperties
import org.slf4j.{ Logger, LoggerFactory }

import scala.util.{ Failure, Success, Try }

case class EventPlusMeta[Event, EvtMeta](event: Event, meta: EvtMeta)

class EnvelopeUtils[Agg, Event, EvtMeta <: EventProperties](formatting: SurgeEventReadFormatting[Event, EvtMeta]) {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  def eventFromBytes(value: Array[Byte]): Option[EventPlusMeta[Event, EvtMeta]] = {
    Try(formatting.readEvent(value)) match {
      case Failure(exception) ⇒
        log.error("Unable to read event from byte array", exception)
        None
      case Success(eventPlusMeta) ⇒
        val event = eventPlusMeta._1
        if (eventPlusMeta._2.isEmpty) {
          log.error("Unable to extract event metadata from byte array")
        }
        eventPlusMeta._2.map { meta ⇒
          EventPlusMeta(event, meta)
        }
    }
  }

}

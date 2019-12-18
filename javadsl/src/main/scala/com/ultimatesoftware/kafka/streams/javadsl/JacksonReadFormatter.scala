package com.ultimatesoftware.kafka.streams.javadsl

import com.fasterxml.jackson.databind.ObjectMapper
import com.ultimatesoftware.kafka.streams.core.SurgeReadFormatting
import com.ultimatesoftware.mp.serialization.envelope.MessagingPlatformEnvelope

object JacksonReadFormatter {
  def emptyEnvelope(): com.ultimatesoftware.mp.serialization.envelope.Envelope = {
    new MessagingPlatformEnvelope()
  }
}

class JacksonReadFormatter[Agg, Event, ENV <: com.ultimatesoftware.mp.serialization.envelope.Envelope](eventTargetClass: Class[Event], aggTargetClass: Class[Agg]) extends SurgeReadFormatting[Agg, Event, ENV] {

  private val jacksonMapper = new ObjectMapper()

  override def readEvent(bytes: Array[Byte]): (Event, ENV) = {
    val event = jacksonMapper.readValue(new String(bytes), eventTargetClass)

    (event, JacksonReadFormatter.emptyEnvelope().asInstanceOf[ENV])
  }

  override def readState(bytes: Array[Byte]): Option[Agg] = {
    val agg = jacksonMapper.readValue(bytes, aggTargetClass)

    Some(agg)
  }
}

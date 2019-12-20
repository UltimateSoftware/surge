// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import com.fasterxml.jackson.databind.ObjectMapper
import com.ultimatesoftware.kafka.streams.core.SurgeReadFormatting

class JacksonReadFormatter[Agg, Event, Envelope](eventTargetClass: Class[Event], aggTargetClass: Class[Agg]) extends SurgeReadFormatting[Agg, Event, Envelope] {

  private val jacksonMapper = new ObjectMapper()

  override def readEvent(bytes: Array[Byte]): (Event, Option[Envelope]) = {
    val event = jacksonMapper.readValue(new String(bytes), eventTargetClass)

    (event, None)
  }

  override def readState(bytes: Array[Byte]): Option[Agg] = {
    val agg = jacksonMapper.readValue(bytes, aggTargetClass)

    Some(agg)
  }
}

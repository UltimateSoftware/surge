// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import com.fasterxml.jackson.databind.ObjectMapper
import com.ultimatesoftware.kafka.streams.core.SurgeAggregateReadFormatting

class JacksonReadFormatter[Agg, Event, EventMetadata](aggTargetClass: Class[Agg]) extends SurgeAggregateReadFormatting[Agg] {

  private val jacksonMapper = new ObjectMapper()

  override def readState(bytes: Array[Byte]): Option[Agg] = {
    val agg = jacksonMapper.readValue(bytes, aggTargetClass)

    Some(agg)
  }
}

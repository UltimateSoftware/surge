// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import com.fasterxml.jackson.databind.ObjectMapper
import com.ultimatesoftware.kafka.streams.core.SurgeAggregateReadFormatting
import com.ultimatesoftware.scala.oss.domain.AggregateSegment
import play.api.libs.json.Json

// TODO Remove this since we should be using ones from surge-mp-domain-helpers
class JacksonReadFormatter[AggId, Agg, Event, EventMetadata](aggTargetClass: Class[Agg]) extends SurgeAggregateReadFormatting[AggId, Agg] {

  private val jacksonMapper = new ObjectMapper()

  override def readState(bytes: Array[Byte]): Option[AggregateSegment[AggId, Agg]] = {
    val agg = jacksonMapper.readValue(bytes, aggTargetClass)
    val aggJson = Json.parse(jacksonMapper.writeValueAsString(agg))

    val segment = AggregateSegment[AggId, Agg]("", aggJson, Some(aggTargetClass))

    Some(segment)
  }
}

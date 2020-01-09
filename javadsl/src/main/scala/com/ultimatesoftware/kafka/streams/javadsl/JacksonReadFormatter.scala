// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import com.ultimatesoftware.kafka.streams.core.SurgeAggregateReadFormatting
import com.ultimatesoftware.scala.core.utils.JsonUtils
import com.ultimatesoftware.scala.oss.domain.AggregateSegment
import play.api.libs.json.JsValue

// TODO Remove this since we should be using ones from surge-mp-domain-helpers
class JacksonReadFormatter[AggId, Agg, Event, EventMetadata](aggTargetClass: Class[Agg]) extends SurgeAggregateReadFormatting[AggId, Agg] {
  override def readState(bytes: Array[Byte]): Option[AggregateSegment[AggId, Agg]] = {
    JsonUtils.parseMaybeCompressedBytes[JsValue](bytes).map { aggJson ⇒
      AggregateSegment[AggId, Agg]("", aggJson, Some(aggTargetClass))
    }
  }
}

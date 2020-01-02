// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import com.ultimatesoftware.kafka.streams.core.SurgeWriteFormatting
import com.ultimatesoftware.scala.core.utils.{ JsonFormats, JsonUtils }
import com.ultimatesoftware.scala.oss.domain.AggregateSegment
import play.api.libs.json.{ JsValue, Json, Writes }

class JacksonWriteFormatter[AggId, Agg, Event, EvtMeta] extends SurgeWriteFormatting[AggId, Agg, Event, EvtMeta] {
  private implicit val eventWriter: Writes[Event] = new Writes[Event] {
    private val jacksonMapper = JsonFormats.genericJacksonMapper

    override def writes(o: Event): JsValue = {
      val objJson = jacksonMapper.writer().writeValueAsString(o)
      Json.parse(objJson)
    }
  }

  private implicit val stateWriter: Writes[Agg] = new Writes[Agg] {
    private val jacksonMapper = JsonFormats.genericJacksonMapper

    override def writes(o: Agg): JsValue = {
      val objJson = jacksonMapper.writer().writeValueAsString(o)
      Json.parse(objJson)
    }
  }

  override def writeEvent(evt: Event, metadata: EvtMeta): Array[Byte] = JsonUtils.gzip(evt)

  override def writeState(agg: AggregateSegment[AggId, Agg]): Array[Byte] = JsonUtils.gzip(agg.value)
}

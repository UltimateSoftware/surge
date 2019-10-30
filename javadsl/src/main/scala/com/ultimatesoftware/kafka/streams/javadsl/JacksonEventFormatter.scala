// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import com.ultimatesoftware.kafka.streams.core.SurgeFormatting
import com.ultimatesoftware.scala.core.utils.{ JsonFormats, JsonUtils }
import play.api.libs.json.{ JsValue, Json, Writes }

class JacksonEventFormatter[Event] extends SurgeFormatting[Event] {
  private implicit val eventWriter: Writes[Event] = new Writes[Event] {
    private val jacksonMapper = JsonFormats.genericJacksonMapper

    override def writes(o: Event): JsValue = {
      val objJson = jacksonMapper.writer().writeValueAsString(o)
      Json.parse(objJson)
    }
  }
  override def writeEvent(evt: Event): Array[Byte] = JsonUtils.gzip(evt)
}

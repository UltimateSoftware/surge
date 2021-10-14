// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import play.api.libs.json.{Format, Json}

import java.io.IOException

trait Serializer[TYPE] {
  def serialize(value: TYPE): Array[Byte]
}

class PlayJsonSerializer[TYPE]()(implicit format: Format[TYPE]) extends Serializer[TYPE] {
  override def serialize(data: TYPE): Array[Byte] = {
    Json.toJson(data).toString().getBytes()
  }
}

class JacksonJsonSerializer[TYPE](objectMapper: ObjectMapper) extends Serializer[TYPE] {
  override def serialize(value: TYPE): Array[Byte] = {
    try {
     objectMapper.writeValueAsBytes(value)
    } catch {
      case error: IOException => throw new RuntimeException(error)
    }
  }
}

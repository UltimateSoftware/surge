// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.internal.utils
import java.util.TimeZone

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{ DeserializationFeature, MapperFeature, ObjectMapper, SerializationFeature }
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import play.api.libs.json._

object JsonFormats {
  val genericJacksonMapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModules(new JavaTimeModule(), new Jdk8Module(), DefaultScalaModule)
    m.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
    m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    m.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    m.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    m.setSerializationInclusion(Include.NON_NULL)
    m.setSerializationInclusion(Include.NON_ABSENT)
    m.setTimeZone(TimeZone.getTimeZone("UTC"))
    m
  }

  def jacksonWriter[T](jacksonMapper: ObjectMapper = genericJacksonMapper): Writes[T] = new Writes[T] {
    override def writes(o: T): JsValue = {
      val objJson = jacksonMapper.writer().writeValueAsString(o)
      Json.parse(objJson)
    }
  }
}

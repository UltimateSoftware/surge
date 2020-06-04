// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.utils

import java.util.{ Currency, TimeZone, UUID }

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper, SerializationFeature }
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import play.api.libs.json._

import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }

object JsonFormats {

  def singletonReads[O](singleton: O): Reads[O] = {
    (__ \ "value").read[String].collect(
      JsonValidationError(s"Expected a JSON object with a single field with key 'value' and value '${singleton.getClass.getSimpleName}'")) {
        case s if s == singleton.getClass.getSimpleName ⇒ singleton
      }
  }
  def singletonWrites[O]: Writes[O] = Writes { singleton ⇒
    Json.obj("value" -> singleton.getClass.getSimpleName)
  }
  def singletonFormat[O](singleton: O): Format[O] = {
    Format(singletonReads(singleton), singletonWrites)
  }

  implicit val uuidReads: Reads[UUID] = implicitly[Reads[String]]
    .collect(JsonValidationError("Invalid UUID"))(Function.unlift { str ⇒
      Try(UUID.fromString(str)).toOption
    })
  implicit val uuidWrites: Writes[UUID] = Writes { uuid ⇒
    JsString(uuid.toString)
  }

  implicit val currencyFormat: Format[Currency] = new Format[Currency] {
    override def reads(json: JsValue): JsResult[Currency] = json.validate[String] match {
      case JsSuccess(value, _) ⇒
        Try(Currency.getInstance(value)).map(currency ⇒ JsSuccess(currency))
          .getOrElse(JsError("Unknown currency code"))
      case JsError(_) ⇒ JsError("Expected currency code string, but got something else")
    }
    override def writes(o: Currency): JsValue = JsString(o.getCurrencyCode)
  }

  implicit val durationReads: Reads[FiniteDuration] = implicitly[Reads[String]]
    .collect(JsonValidationError("Invalid duration"))(Function.unlift { str ⇒
      Some(Duration(str)).collect { case fd: FiniteDuration ⇒ fd }
    })

  implicit val durationWrites: Writes[FiniteDuration] = Writes { duration ⇒
    JsString(duration.toString)
  }

  implicit val jodaDateTimeReads: Reads[DateTime] = implicitly[Reads[String]]
    .collect(JsonValidationError("Invalid ISO Date Time"))(Function.unlift { str ⇒
      Try(DateTime.parse(str, ISODateTimeFormat.dateTime())).toOption
    })

  implicit val jodaDateTimeWrites: Writes[DateTime] = Writes { dateTime ⇒
    JsString(dateTime.toString(ISODateTimeFormat.dateTime()))
  }

  def genericJacksonMapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModules(new JavaTimeModule(), new Jdk8Module(), new KotlinModule(), DefaultScalaModule)
    m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    m.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    m.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    m.setSerializationInclusion(Include.NON_NULL)
    m.setTimeZone(TimeZone.getTimeZone("UTC"))
    m
  }

  def jacksonWriter[T]: Writes[T] = new Writes[T] {
    private val jacksonMapper = genericJacksonMapper

    override def writes(o: T): JsValue = {
      val objJson = jacksonMapper.writer().writeValueAsString(o)
      Json.parse(objJson)
    }
  }

  /**
   * Creates a play json formatter that just leverages an underlying jackson json formatter.
   * This is useful for providing a json serializer for some of the underlying java libraries
   * that we're leveraging that already have a jackson json formatter.
   * @param classTag implicit ClassTag for the type T to be serialized
   * @tparam T The class with jackson formatting to create a play json formatter for
   * @return A new json formatter for type T
   */
  def jsonFormatterFromJackson[T](implicit classTag: ClassTag[T]): Format[T] = new Format[T] {
    private val jacksonMapper = genericJacksonMapper

    override def writes(o: T): JsValue = jacksonWriter[T].writes(o)

    override def reads(json: JsValue): JsResult[T] = {
      val maybeDeserialized = Try(jacksonMapper.readerFor(classTag.runtimeClass).readValue[T](json.toString))

      maybeDeserialized match {
        case Success(value)     ⇒ JsSuccess(value)
        case Failure(exception) ⇒ JsError(exception.getMessage)
      }
    }
  }
}

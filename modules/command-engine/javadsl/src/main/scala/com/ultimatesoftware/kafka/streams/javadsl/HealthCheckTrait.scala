// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import com.fasterxml.jackson.annotation.JsonProperty

import scala.collection.JavaConverters._
import com.ultimatesoftware.kafka.streams.{ HealthCheck ⇒ ScalaHealthCheck }
import com.ultimatesoftware.scala.core.utils.JsonFormats

import scala.compat.java8.OptionConverters._

class HealthCheck(
    val name: String,
    val id: String,
    val status: String,
    val components: Optional[java.util.List[HealthCheck]],
    val details: Optional[java.util.Map[String, String]])

object HealthCheck {
  implicit val writer = JsonFormats.jacksonWriter[HealthCheck]
  implicit class HealthCheckToJavaConverter(scalaHealthCheck: ScalaHealthCheck) {
    def asJava: HealthCheck = {
      new HealthCheck(
        scalaHealthCheck.name,
        scalaHealthCheck.id,
        scalaHealthCheck.status,
        scalaHealthCheck.components.map(scalaHealthCheckList ⇒
          scalaHealthCheckList.map(scalaHealthCheck ⇒
            scalaHealthCheck.asJava).asJava).asJava,
        scalaHealthCheck.details.map(_.asJava).asJava)
    }
  }
}

trait HealthCheckTrait {
  def getHealthCheck(): CompletionStage[HealthCheck]
}

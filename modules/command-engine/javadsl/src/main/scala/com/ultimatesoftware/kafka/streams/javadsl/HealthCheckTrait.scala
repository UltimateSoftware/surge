// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util
import java.util.concurrent.CompletionStage
import com.ultimatesoftware.kafka.streams.{ HealthCheck ⇒ ScalaHealthCheck }
import com.ultimatesoftware.scala.core.utils.JsonFormats

import scala.collection.JavaConverters._

class HealthCheck(
    val name: String,
    val id: String,
    val status: String,
    val isHealthy: Boolean,
    val components: java.util.List[HealthCheck],
    val details: java.util.Map[String, String])

object HealthCheck {
  implicit val writer = JsonFormats.jacksonWriter[HealthCheck]
  implicit class HealthCheckToJavaConverter(scalaHealthCheck: ScalaHealthCheck) {
    def asJava: HealthCheck = {
      new HealthCheck(
        scalaHealthCheck.name,
        scalaHealthCheck.id,
        scalaHealthCheck.status,
        scalaHealthCheck.isHealthy.getOrElse(false),
        scalaHealthCheck.components.map(scalaHealthCheckList ⇒
          scalaHealthCheckList.map(scalaHealthCheck ⇒
            scalaHealthCheck.asJava).asJava).getOrElse(new util.ArrayList()),
        scalaHealthCheck.details.map(_.asJava).getOrElse(new util.HashMap()))
    }
  }
}

trait HealthCheckTrait {
  def getHealthCheck(): CompletionStage[HealthCheck]
}

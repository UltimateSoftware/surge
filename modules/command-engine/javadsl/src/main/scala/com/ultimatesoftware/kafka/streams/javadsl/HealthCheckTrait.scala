// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import scala.collection.JavaConverters._
import com.ultimatesoftware.kafka.streams.{ HealthCheck ⇒ ScalaHealthCheck }
import scala.compat.java8.OptionConverters._

case class HealthCheck(
    name: String,
    id: String,
    status: String,
    components: Optional[java.util.List[HealthCheck]],
    details: Optional[java.util.Map[String, String]])

object HealthCheck {
  implicit class HealthCheckToJavaConverter(scalaHealthCheck: ScalaHealthCheck) {
    def asJava: HealthCheck = {
      HealthCheck(
        name = scalaHealthCheck.name,
        id = scalaHealthCheck.id,
        status = scalaHealthCheck.status,
        components = scalaHealthCheck.components.map(scalaHealthCheckList ⇒
          scalaHealthCheckList.map(scalaHealthCheck ⇒
            scalaHealthCheck.asJava).asJava).asJava,
        details = scalaHealthCheck.details.map(_.asJava).asJava)
    }
  }
}

trait HealthCheckTrait {
  def getHealthCheck(): CompletionStage[HealthCheck]
}

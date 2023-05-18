// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.javadsl.common

import surge.internal.health

import java.util
import scala.jdk.CollectionConverters._

class HealthCheck(
    val name: String,
    val id: String,
    val status: String,
    val isHealthy: Boolean,
    val components: java.util.List[HealthCheck],
    val details: java.util.Map[String, String])

object HealthCheck {
  implicit class HealthCheckToJavaConverter(scalaHealthCheck: health.HealthCheck) {
    def asJava: HealthCheck = {
      new HealthCheck(
        scalaHealthCheck.name,
        scalaHealthCheck.id,
        scalaHealthCheck.status,
        scalaHealthCheck.isHealthy.getOrElse(false),
        scalaHealthCheck.components
          .map(scalaHealthCheckList => scalaHealthCheckList.map(scalaHealthCheck => scalaHealthCheck.asJava).asJava)
          .getOrElse(new util.ArrayList()),
        scalaHealthCheck.details.map(_.asJava).getOrElse(new util.HashMap()))
    }
  }
}

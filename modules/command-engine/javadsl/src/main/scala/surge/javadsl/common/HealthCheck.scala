//
package surge.javadsl.common

import play.api.libs.json.Writes
import surge.internal.utils.JsonFormats
import surge.kafka.streams.{ HealthCheck => ScalaHealthCheck }

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
  implicit val writer: Writes[HealthCheck] = JsonFormats.jacksonWriter[HealthCheck]
  implicit class HealthCheckToJavaConverter(scalaHealthCheck: ScalaHealthCheck) {
    def asJava: HealthCheck = {
      new HealthCheck(
        scalaHealthCheck.name,
        scalaHealthCheck.id,
        scalaHealthCheck.status,
        scalaHealthCheck.isHealthy.getOrElse(false),
        scalaHealthCheck.components.map(scalaHealthCheckList =>
          scalaHealthCheckList.map(scalaHealthCheck =>
            scalaHealthCheck.asJava).asJava).getOrElse(new util.ArrayList()),
        scalaHealthCheck.details.map(_.asJava).getOrElse(new util.HashMap()))
    }
  }
}

// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import org.slf4j.LoggerFactory
import play.api.libs.json.{ Format, Json }

import scala.concurrent.{ ExecutionContext, Future }

class SurgeHealthCheck(healthCheckId: String, components: HealthyComponent*)(implicit executionContext: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  def healthCheck(): Future[HealthCheck] = {
    Future.sequence(components.map { healthyComponent ⇒
      healthyComponent.healthCheck()
    }).map { healthyComponentChecks ⇒
      val isUp = healthyComponentChecks.forall(_.status.equals(HealthCheckStatus.UP))
      HealthCheck(
        name = "surge",
        id = healthCheckId,
        status = if (isUp) HealthCheckStatus.UP else HealthCheckStatus.DOWN,
        components = Some(healthyComponentChecks))
    }.recoverWith {
      case err: Throwable ⇒
        log.error(s"Fail to get surge health check", err)
        Future.successful(
          HealthCheck(
            name = "surge",
            id = healthCheckId,
            status = HealthCheckStatus.DOWN))
    }
  }

}

trait HealthyComponent {
  def healthCheck(): Future[HealthCheck]
}

case class HealthCheck(
    name: String,
    id: String,
    status: String,
    components: Option[Seq[HealthCheck]] = None,
    details: Option[Map[String, String]] = None) {
  require(HealthCheckStatus.validStatuses.contains(status))
}

object HealthCheck {
  implicit val format: Format[HealthCheck] = Json.format
}

object HealthCheckStatus {
  val UP = "up"
  val DOWN = "down"

  val validStatuses = Seq(HealthCheckStatus.UP, HealthCheckStatus.DOWN)
}

object HealthyActor {
  case object GetHealth
}

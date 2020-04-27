// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import play.api.libs.json.{ Format, Json }

import scala.concurrent.{ ExecutionContext, Future }

class SurgeHealthCheck(components: HealthyComponent*)(implicit executionContext: ExecutionContext) {
  def healthCheck(): Future[HealthCheck] = {
    Future.sequence(components.map { healthyComponent ⇒
      healthyComponent.healthCheck()
    }).map { healthyComponentChecks ⇒
      HealthCheck(
        name = "Surge",
        running = healthyComponentChecks.forall(_.running),
        components = Some(healthyComponentChecks))
    }.recoverWith {
      case err: Throwable ⇒
        Future.successful(
          HealthCheck(
            name = "Surge",
            running = false,
            message = Some(err.getMessage)))
    }
  }

}

trait HealthyComponent {
  def healthCheck(): Future[HealthCheck]
}

case class HealthCheck(
    name: String,
    // com.ultimatesoftware.scala.core.monitoring.HealthCheckStatus vs Boolean isHealthy???
    running: Boolean,
    components: Option[Seq[HealthCheck]] = None,
    message: Option[String] = None,
    details: Option[Map[String, String]] = None)

object HealthCheck {
  implicit val format: Format[HealthCheck] = Json.format
}

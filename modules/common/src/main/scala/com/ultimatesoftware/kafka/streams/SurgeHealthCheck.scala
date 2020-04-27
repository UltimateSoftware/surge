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
        isHealthy = healthyComponentChecks.forall(_.isHealthy),
        responseTime = calculateResponseTime(healthyComponentChecks),
        components = healthyComponentChecks,
        message = None)
    }.recoverWith {
      case err: Throwable ⇒
        Future.successful(
          HealthCheck(
            name = "Surge",
            isHealthy = false,
            responseTime = None,
            components = Seq(),
            message = Some(err.getMessage)))
    }
  }

  private def calculateResponseTime(components: Seq[HealthCheck]): Option[Long] = {
    components.foldLeft(Option.empty[Long])((a, b) ⇒
      if (a.isDefined || b.responseTime.isDefined) {
        val time = a.getOrElse(0.toLong) + b.responseTime.getOrElse(0.toLong)
        Some(time)
      } else {
        None
      })
  }

}

trait HealthyComponent {
  def healthCheck(): Future[HealthCheck]
}

// TODO: Evaluate
//  - a list of applied configurations as a Map[String, String] could be useful
case class HealthCheck(
    name: String,
    // com.ultimatesoftware.scala.core.monitoring.HealthCheckStatus vs Boolean isHealthy???
    isHealthy: Boolean,
    responseTime: Option[Long],
    components: Seq[HealthCheck],
    message: Option[String])

object HealthCheck {
  implicit val format: Format[HealthCheck] = Json.format
}

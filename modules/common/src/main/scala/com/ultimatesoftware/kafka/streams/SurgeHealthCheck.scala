// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

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
      val baseHealthTree = HealthCheck(
        name = "surge",
        id = healthCheckId,
        status = if (isUp) HealthCheckStatus.UP else HealthCheckStatus.DOWN,
        components = Some(healthyComponentChecks))
      healthyTree(baseHealthTree)
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

  private def healthyTree(node: HealthCheck): HealthCheck = {
    val childs = node.components.map(_.map(n ⇒ healthyTree(n)))
    val isHealthy = node.status == HealthCheckStatus.UP && (childs.isEmpty || childs.forall(_.forall(_.isHealthy.contains(true))))
    val newStatus = if (isHealthy) {
      HealthCheckStatus.UP
    } else {
      HealthCheckStatus.DOWN
    }

    node.copy(
      components = childs,
      status = newStatus,
      isHealthy = Some(isHealthy))
  }
}

trait HealthyComponent {
  def healthCheck(): Future[HealthCheck]
}

case class HealthCheck(
    name: String,
    id: String,
    status: String,
    isHealthy: Option[Boolean] = None,
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

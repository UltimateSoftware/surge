// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import akka.actor.NoSerializationVerificationNeeded
import org.slf4j.LoggerFactory
import play.api.libs.json.{ Format, Json }
import surge.core.Controllable

import java.util.regex.Pattern
import scala.concurrent.{ ExecutionContext, Future }

class SurgeHealthCheck(healthCheckId: String, components: HealthyComponent*)(implicit executionContext: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  def healthCheck(): Future[HealthCheck] = {
    Future
      .sequence(components.map { healthyComponent =>
        healthyComponent.healthCheck()
      })
      .map { healthyComponentChecks =>
        val isUp = healthyComponentChecks.forall(_.status.equals(HealthCheckStatus.UP))
        val baseHealthTree = HealthCheck(
          name = "surge",
          id = healthCheckId,
          status = if (isUp) HealthCheckStatus.UP else HealthCheckStatus.DOWN,
          components = Some(healthyComponentChecks))
        healthyTree(baseHealthTree)
      }
      .recoverWith { case err: Throwable =>
        log.error(s"Fail to get surge health check", err)
        Future.successful(HealthCheck(name = "surge", id = healthCheckId, status = HealthCheckStatus.DOWN))
      }
  }

  private def healthyTree(node: HealthCheck): HealthCheck = {
    val childs = node.components.map(_.map(n => healthyTree(n)))
    val isHealthy = node.status == HealthCheckStatus.UP && (childs.isEmpty || childs.forall(_.forall(_.isHealthy.contains(true))))
    val newStatus = if (isHealthy) {
      HealthCheckStatus.UP
    } else {
      HealthCheckStatus.DOWN
    }

    node.copy(components = childs, status = newStatus, isHealthy = Some(isHealthy))
  }
}

trait HealthyComponent {

  /**
   * Perform a Health Check
   * @return
   *   HealthCheck
   */
  def healthCheck(): Future[HealthCheck]

  /**
   * Perform a readiness check
   * @return
   *   HealthCheck
   */
  def readiness(): Future[HealthCheck] = Future.successful(HealthCheck(name = "NA", id = "NA", status = HealthCheckStatus.UP))

  /**
   * Signals Patterns that will trigger a restart
   * @return
   *   Seq[Pattern]
   */
  def restartSignalPatterns(): Seq[Pattern] = Seq.empty

  /**
   * Signal Patterns that will trigger a shutdown
   * @return
   *   Seq[Pattern]
   */
  def shutdownSignalPatterns(): Seq[Pattern] = Seq.empty

  private[surge] def controllable: Controllable
}

case class HealthCheck(
    name: String,
    id: String,
    status: String,
    isHealthy: Option[Boolean] = None,
    components: Option[Seq[HealthCheck]] = None,
    details: Option[Map[String, String]] = None)
    extends NoSerializationVerificationNeeded {
  require(HealthCheckStatus.validStatuses.contains(status))
}

object HealthCheck {
  implicit val format: Format[HealthCheck] = Json.format
}

object HealthCheckStatus {
  val UP = "up"
  val DOWN = "down"

  val validStatuses: Seq[String] = Seq(HealthCheckStatus.UP, HealthCheckStatus.DOWN)
}

object HealthyActor {
  case object GetHealth extends NoSerializationVerificationNeeded
}

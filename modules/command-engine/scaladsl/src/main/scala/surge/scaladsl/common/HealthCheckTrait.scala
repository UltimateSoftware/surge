// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

//
package surge.scaladsl.common

import surge.internal.health.HealthCheck

import scala.concurrent.Future

trait HealthCheckTrait {
  def healthCheck: Future[HealthCheck]
  def readiness: Future[HealthCheck]
}

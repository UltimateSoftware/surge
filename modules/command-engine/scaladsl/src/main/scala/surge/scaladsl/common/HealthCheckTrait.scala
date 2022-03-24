// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

//
package surge.scaladsl.common

import surge.internal.health.HealthCheck

import scala.concurrent.Future

trait HealthCheckTrait {
  def healthCheck: Future[HealthCheck]
  def ready: Future[HealthCheck]
}

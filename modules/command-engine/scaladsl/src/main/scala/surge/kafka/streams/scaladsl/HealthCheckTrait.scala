// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams.scaladsl

import surge.kafka.streams.HealthCheck

import scala.concurrent.Future

trait HealthCheckTrait {
  def healthCheck(): Future[HealthCheck]
}
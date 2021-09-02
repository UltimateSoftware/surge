// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.common

import surge.kafka.streams.HealthCheck

import scala.concurrent.Future

trait HealthCheckTrait {
  def healthCheck: Future[HealthCheck]
}

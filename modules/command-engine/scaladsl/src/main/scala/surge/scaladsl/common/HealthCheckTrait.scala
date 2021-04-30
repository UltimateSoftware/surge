//
package surge.scaladsl.common

import surge.kafka.streams.HealthCheck

import scala.concurrent.Future

trait HealthCheckTrait {
  def healthCheck: Future[HealthCheck]
}

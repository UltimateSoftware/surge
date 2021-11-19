// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import akka.Done
import surge.health.domain.HealthRegistration

import scala.util.Try

trait RegistrationHandler {
  def handle(registration: HealthRegistration): Try[Done]
}

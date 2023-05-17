// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import akka.Done
import surge.health.HealthRegistration

import scala.util.Try

trait RegistrationHandler {
  def handle(registration: HealthRegistration): Try[Done]
}

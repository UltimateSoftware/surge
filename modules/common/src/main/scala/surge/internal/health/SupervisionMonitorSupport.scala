// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import surge.health.{ HealthRegistrationReceived, HealthSignalReceived }

trait SupervisionMonitorSupport {
  def healthSignalReceived(signal: HealthSignalReceived): Unit
  def registrationReceived(registration: HealthRegistrationReceived): Unit
}

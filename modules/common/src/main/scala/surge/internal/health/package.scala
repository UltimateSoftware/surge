// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge

import surge.internal.health.HealthSignalBusInternal
import surge.internal.health.supervisor.HealthSupervisorActorRef

package object health {
  object SignalType extends Enumeration {
    val ERROR, WARNING, TRACE = Value
  }

  type SupervisorProvider = HealthSignalBusInternal => HealthSupervisorActorRef
}

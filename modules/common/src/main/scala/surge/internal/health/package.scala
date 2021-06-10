// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge

import org.slf4j.{ Logger, LoggerFactory }
import surge.internal.health.HealthSignalBusInternal
import surge.internal.health.supervisor.HealthSupervisorActorRef

package object health {
  object SignalType extends Enumeration {
    val ERROR, WARNING, TRACE, OTHER = Value
  }

  val log: Logger = LoggerFactory.getLogger(getClass)
  type SupervisorProvider = HealthSignalBusInternal => HealthSupervisorActorRef
}

// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.jmx

import akka.actor.{ Actor, ActorLogging, SupervisorStrategy }

import javax.management.{ JMException, JMRuntimeException }

trait ActorJMXSupervisor extends Actor with ActorLogging {

  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._

  import scala.concurrent.duration._

  override val supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case e: JMRuntimeException =>
        log.error(e, "Supervisor strategy STOPPING actor from errors during JMX invocation")
        Stop
      case e: JMException =>
        log.error(e, "Supervisor strategy STOPPING actor from incorrect invocation of JMX registration")
        Stop
      case t =>
        // Use the default supervisor strategy otherwise.
        super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }
}

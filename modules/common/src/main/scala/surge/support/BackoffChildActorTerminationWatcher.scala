// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.support

import akka.actor.{ Actor, ActorRef, Props, Terminated }
import akka.pattern.BackoffSupervisor

/**
 * There is no documented way to be notify once an actor is killed by the Backoff supervisor
 * once it meets the max number of retries
 * This is a custom implementation that will notify
 * @param subject The BackoffSupervisor actor
 * @param onChildTermination The function to call when the actor inside `subject` gets terminated
 */
class BackoffChildActorTerminationWatcher(subject: ActorRef, onChildTermination: () ⇒ Unit) extends Actor with Logging {

  override def receive: Receive = {
    case BackoffSupervisor.CurrentChild(Some(ref)) ⇒
      context.watch(ref)
      log.info(s"Actor $ref termination watcher started for ${subject.path}")
    case Terminated(ref) ⇒
      log.debug(s"Actor $ref is terminated")
      onChildTermination()
    case _ ⇒
    // ignore
  }

  override def preStart(): Unit = {
    subject ! BackoffSupervisor.GetCurrentChild
    super.preStart()
  }

}

object BackoffChildActorTerminationWatcher {
  def props(subject: ActorRef, f: () ⇒ Unit): Props =
    Props(new BackoffChildActorTerminationWatcher(subject, f))
}

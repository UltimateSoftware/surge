// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.actor

import akka.actor.{ Actor, ActorRef, Props, Terminated }

object ActorLifecycleManagerActor {
  case object Start
  case object Stop

  def apply(managedActorProps: Props, managedActorName: Option[String]): ActorLifecycleManagerActor = {
    new ActorLifecycleManagerActor(managedActorProps, managedActorName)
  }
}

class ActorLifecycleManagerActor(managedActorProps: Props, managedActorName: Option[String] = None) extends Actor {
  override def receive: Receive = stopped

  private def stopped: Receive = {
    case ActorLifecycleManagerActor.Start =>
      val actor = managedActorName match {
        case Some(name) => context.actorOf(managedActorProps, name)
        case _          => context.actorOf(managedActorProps)
      }
      context.watch(actor)
      context.become(running(actor))
    case msg => context.system.deadLetters ! msg
  }

  private def running(managedActor: ActorRef): Receive = {
    case ActorLifecycleManagerActor.Stop =>
      context.stop(managedActor)
      context.become(stopped)
    case Terminated(actorRef) if actorRef == managedActor => context.become(stopped)
    case msg                                              => managedActor.forward(msg)
  }
}

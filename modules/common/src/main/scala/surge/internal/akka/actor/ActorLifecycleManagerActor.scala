// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.actor

import akka.actor.{ Actor, ActorRef, Props, Terminated }
import surge.core.Ack

object ActorLifecycleManagerActor {
  case object Start
  case object Stop
  def apply(managedActorProps: Props, managedActorName: Option[String]): ActorLifecycleManagerActor = {
    new ActorLifecycleManagerActor(managedActorProps, managedActorName)
  }
}

class ActorLifecycleManagerActor(
    managedActorProps: Props,
    managedActorName: Option[String] = None,
    initMessage: Option[() => Any] = None,
    finalizeMessage: Option[() => Any] = None)
    extends Actor {
  override def receive: Receive = stopped

  private def stopped: Receive = {
    case ActorLifecycleManagerActor.Start =>
      val actor = managedActorName match {
        case Some(name) =>
          val actorRef = context.actorOf(managedActorProps, name)
          actorRef
        case _ => context.actorOf(managedActorProps)
      }

      initMessage.foreach(init => actor ! init())

      context.watch(actor)
      context.become(running(actor))
      sender() ! Ack()
    case ActorLifecycleManagerActor.Stop =>
      sender() ! Ack()
    case msg => context.system.deadLetters ! msg
  }

  private def running(managedActor: ActorRef): Receive = {
    case ActorLifecycleManagerActor.Start =>
      sender() ! Ack()
    case ActorLifecycleManagerActor.Stop =>
      finalizeMessage match {
        case Some(fin) => managedActor ! fin()
        case None =>
          context.stop(managedActor)
      }
      context.become(stopped)
      sender() ! Ack()
    case Terminated(actorRef) if actorRef == managedActor => context.become(stopped)
    case msg                                              => managedActor.forward(msg)
  }
}

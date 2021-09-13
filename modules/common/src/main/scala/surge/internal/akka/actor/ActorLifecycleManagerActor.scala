// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.actor

import akka.actor.{ Actor, ActorRef, Props, Terminated }
import akka.pattern._
import org.slf4j.LoggerFactory
import surge.core.Ack
import surge.internal.akka.ActorOps
import surge.internal.config.TimeoutConfig

import scala.concurrent.Await
import scala.concurrent.duration._

object ActorLifecycleManagerActor {
  case object Start
  case object Stop
  case object GetManagedActorPath
  val defaultStopTimeout: FiniteDuration = 30.seconds
}

class ActorLifecycleManagerActor(
    managedActorProps: Props,
    componentName: String,
    managedActorName: Option[String] = None,
    startMessageAdapter: Option[() => Any] = None,
    stopMessageAdapter: Option[() => Any] = None)
    extends Actor
    with ActorOps {
  import ActorLifecycleManagerActor._
  private val log = LoggerFactory.getLogger(getClass)

  override def receive: Receive = stopped

  private def stopped: Receive = {
    case ActorLifecycleManagerActor.Start =>
      val actor = managedActorName match {
        case Some(name) =>
          val actorRef = context.actorOf(managedActorProps, name)
          actorRef
        case _ => context.actorOf(managedActorProps)
      }

      startMessageAdapter.foreach(init => actor ! init())

      log.info("Lifecycle manager starting actor named {} for component {}", Seq(actor.prettyPrintPath, componentName): _*)
      context.watch(actor)
      context.become(running(actor))
      sender() ! Ack()
    case ActorLifecycleManagerActor.Stop =>
      sender() ! Ack()
    case msg =>
      context.system.deadLetters ! msg
  }

  private def running(managedActor: ActorRef): Receive = {
    case ActorLifecycleManagerActor.GetManagedActorPath =>
      sender() ! managedActor.path
    case ActorLifecycleManagerActor.Start =>
      sender() ! Ack()
    case ActorLifecycleManagerActor.Stop =>
      log.info("Lifecycle manager stopping actor named {} for component {}", Seq(managedActor.prettyPrintPath, componentName): _*)
      val stoppedActor = stopMessageAdapter match {
        case Some(fin) =>
          gracefulStop(managedActor, defaultStopTimeout, fin())
        case None =>
          gracefulStop(managedActor, defaultStopTimeout)
      }
      Await.result(stoppedActor, TimeoutConfig.LifecycleManagerActor.askTimeout)
      context.become(stopped)
      sender() ! Ack()
    case Terminated(actorRef) if actorRef == managedActor =>
      log.info("Lifecycle manager saw actor named {} stop for component {}", Seq(managedActor.prettyPrintPath, componentName): _*)
      context.become(stopped)
    case msg => managedActor.forward(msg)
  }
}

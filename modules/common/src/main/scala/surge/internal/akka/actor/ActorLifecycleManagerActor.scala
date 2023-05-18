// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.actor

import akka.actor.{ Actor, ActorPath, ActorRef, ActorSystem, NoSerializationVerificationNeeded, Props, Terminated }
import akka.pattern._
import akka.util.Timeout
import org.slf4j.LoggerFactory
import surge.core.Ack
import surge.internal.akka.ActorOps
import surge.internal.akka.actor.ActorLifecycleManagerActor.GetManagedActorPath
import surge.internal.config.TimeoutConfig

import scala.concurrent.Future
import scala.concurrent.duration._

case class ManagedActorRef(ref: ActorRef) {
  implicit val timeout: Timeout = TimeoutConfig.LifecycleManagerActor.askTimeout
  def start(): Future[Ack] = {
    ref.ask(ActorLifecycleManagerActor.Start).mapTo[Ack]
  }

  def stop(): Future[Ack] = {
    ref.ask(ActorLifecycleManagerActor.Stop).mapTo[Ack]
  }

  def managedPath(): Future[Option[ActorPath]] = {
    ref.ask(GetManagedActorPath)(30.seconds).mapTo[Option[ActorPath]]
  }
}

object ActorLifecycleManagerActor {
  val defaultStopTimeout: FiniteDuration = 30.seconds

  def manage(
      actorSystem: ActorSystem,
      managedActorProps: Props,
      componentName: String,
      managedActorName: Option[String] = None,
      actorLifecycleName: Option[String] = None,
      startMessageAdapter: Option[() => Any] = None,
      stopMessageAdapter: Option[() => Any] = None): ManagedActorRef = {
    val lifecycleManagerProps = Props(
      new ActorLifecycleManagerActor(managedActorProps, componentName, managedActorName, startMessageAdapter, stopMessageAdapter))
    val actor = actorLifecycleName match {
      case Some(name) => actorSystem.actorOf(lifecycleManagerProps, name)
      case None       => actorSystem.actorOf(lifecycleManagerProps)
    }
    ManagedActorRef(actor)
  }

  case object Start extends NoSerializationVerificationNeeded
  case object Stop extends NoSerializationVerificationNeeded
  case object GetManagedActorPath extends NoSerializationVerificationNeeded
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
      sender() ! Ack
    case ActorLifecycleManagerActor.Stop =>
      sender() ! Ack
    case ActorLifecycleManagerActor.GetManagedActorPath =>
      sender() ! None
    case msg =>
      context.system.deadLetters ! msg
  }

  private def running(managedActor: ActorRef): Receive = {
    case ActorLifecycleManagerActor.GetManagedActorPath =>
      sender() ! Some(managedActor.path)
    case ActorLifecycleManagerActor.Start =>
      sender() ! Ack
    case ActorLifecycleManagerActor.Stop =>
      log.info("Lifecycle manager stopping actor named {} for component {}", Seq(managedActor.prettyPrintPath, componentName): _*)
      stopMessageAdapter match {
        case Some(fin) =>
          gracefulStop(managedActor, defaultStopTimeout, fin())
        case None =>
          gracefulStop(managedActor, defaultStopTimeout)
      }
      sender() ! Ack
      context.become(stopped)
    case Terminated(actorRef) if actorRef == managedActor =>
      log.info("Lifecycle manager saw actor named {} stop for component {}", Seq(managedActor.prettyPrintPath, componentName): _*)
      context.become(stopped)
    case msg => managedActor.forward(msg)
  }
}

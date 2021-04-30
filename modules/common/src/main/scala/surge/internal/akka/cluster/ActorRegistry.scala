// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.cluster

import akka.Done
import akka.actor.{ Actor, ActorPath, ActorRef, ActorSystem, Address, Props, Terminated }
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import surge.akka.cluster.JacksonSerializable
import surge.internal.akka.cluster.Receptionist._
import surge.internal.config.TimeoutConfig
import surge.internal.utils.Logging
import surge.kafka.HostPort

import scala.concurrent.{ ExecutionContext, Future }

object Receptionist {
  sealed trait ReceptionistRequest extends JacksonSerializable
  case class RegisterService(key: String, actor: ActorRef, tags: List[String]) extends ReceptionistRequest
  case class GetServicesById(key: String) extends ReceptionistRequest

  sealed trait ReceptionistResponse extends JacksonSerializable
  case class ServicesList(address: Address, records: List[Record]) extends ReceptionistResponse
  case object ServiceRegistered extends ReceptionistResponse

  case class Record(actorPath: String, tags: List[String] = List())
}
class Receptionist(systemAddress: Address) extends Actor {
  private val log = LoggerFactory.getLogger(getClass)
  override def receive: Receive = registry(Map())

  private def registry(inventory: Map[String, List[Record]]): Receive = {
    case RegisterService(key, actor, tags) =>
      context.watch(actor)
      val record = Record(actor.path.toString, tags)
      val newRecords = inventory.getOrElse(key, List.empty) :+ record
      val newInventory = inventory ++ Map(key -> newRecords)
      context.become(registry(newInventory))
      sender() ! ServiceRegistered
    case GetServicesById(key) =>
      sender() ! ServicesList(systemAddress, inventory.getOrElse(key, List()))
    case Terminated(ref) =>
      log.debug("Receptionist saw actor at path [{}] terminate. Removing it from the inventory", ref.path)
      val newInventory = removeFromInventory(inventory, ref)
      context.become(registry(newInventory))
  }

  private def removeFromInventory(inventory: Map[String, List[Record]], actorToRemove: ActorRef): Map[String, List[Record]] = {
    inventory.map {
      case (key, list) =>
        val newList = list.filterNot(record => record.actorPath.equals(actorToRemove.path.toString))
        key -> newList
    }
      .filter { case (_, list) => list.nonEmpty }
  }
}

class ActorRegistry(val actorSystem: ActorSystem) extends Logging with ActorSystemHostAwareness {
  private val receptionistActorName = "actor-registry"
  private lazy val receptionistLocalPath = s"akka://${actorSystem.name}/user/$receptionistActorName"

  implicit val askTimeout: Timeout = TimeoutConfig.ActorRegistry.askTimeout

  private def findReceptionist(path: String = receptionistLocalPath)(implicit executionContext: ExecutionContext): Future[Option[ActorRef]] = {
    actorSystem.actorSelection(path).resolveOne(TimeoutConfig.ActorRegistry.resolveActorTimeout).map(Some(_)).recoverWith {
      case _ if path equals receptionistLocalPath =>
        ActorPath.fromString(path)
        log.debug(s"Receptionist not found for path $path, initializing local ActorRegistry")
        val receptionistActor: ActorRef = actorSystem.actorOf(Props(new Receptionist(localAddress)), receptionistActorName)
        Future.successful(Some(receptionistActor))
      case _ =>
        Future.successful(None) // no remote receptionist found
    }
  }

  def registerService(key: String, actor: ActorRef, tags: List[String] = List())(implicit executionContext: ExecutionContext): Future[Done] = {
    findReceptionist(receptionistLocalPath).flatMap {
      case Some(receptionist) =>
        receptionist.ask(Receptionist.RegisterService(key, actor, tags)).map { _ =>
          log.debug("Successfully registered actor for key {}", key)
          Done
        }
      case _ =>
        log.error("Actor Registry is not available for local services")
        Future.successful(Done)
    }
  }

  private def discoverRecords(key: String, queryActorSystems: List[HostPort])(implicit executionContext: ExecutionContext): Future[List[Record]] = {
    findAllReceptionists(queryActorSystems).flatMap { receptionists =>
      Future.sequence(
        receptionists.map { actorRef =>
          actorRef.ask(Receptionist.GetServicesById(key))
            .mapTo[Receptionist.ServicesList]
            .map { servicesList =>
              servicesList.records.map { record =>
                record.copy(actorPath = remotePath(record.actorPath, servicesList.address))
              }
            }
        }).map(listOfLists => listOfLists.flatten)
    }
  }

  def discoverActors(key: String, queryActorSystems: List[HostPort])(implicit executionContext: ExecutionContext): Future[List[String]] = {
    discoverRecords(key, queryActorSystems).map(records => records.map(_.actorPath))
  }

  def discoverActors(
    key: String,
    queryActorSystems: List[HostPort],
    tags: List[String])(implicit executionContext: ExecutionContext): Future[List[String]] = {
    discoverRecords(key, queryActorSystems).map { records =>
      records.filter(_.tags.intersect(tags).nonEmpty)
    }.map(records => records.map(_.actorPath))
  }

  private def findAllReceptionists(
    queryActorSystems: List[HostPort])(implicit executionContext: ExecutionContext): Future[List[ActorRef]] = Future.sequence {
    queryActorSystems.map {
      case hostPort if isHostPortThisNode(hostPort) =>
        findReceptionist()
      case hostPort =>
        val path = remotePath(receptionistLocalPath, hostPort)
        findReceptionist(path)
    }
  }.map(_.flatten)
}

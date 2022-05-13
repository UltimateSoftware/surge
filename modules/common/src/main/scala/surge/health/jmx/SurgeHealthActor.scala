// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.jmx

import akka.actor.ActorRef
import akka.pattern.ask
import surge.health.jmx.Api._
import surge.health.jmx.Domain.HealthRegistrationDetail
import surge.health.jmx.View.{ HealthRegistrationDetailMxView, HealthRegistryMxView }
import surge.health.supervisor.Api.{ QueryComponentExists, RestartComponent, ShutdownComponent, StartComponent }
import surge.internal.config.TimeoutConfig
import surge.jmx.ActorWithJMX

import java.util
import javax.management.openmbean.CompositeData
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.{ Failure, Success }

object SurgeHealthActor {
  val managementName: String = "SurgeHealthMBean"
  def apply(supervisorRef: ActorRef): SurgeHealthActor = {
    new SurgeHealthActor(supervisorRef)
  }
}

case class SurgeHealthState(registry: Seq[HealthRegistrationDetail] = Seq[HealthRegistrationDetail]())

class SurgeHealthActor(val supervisorRef: ActorRef) extends ActorWithJMX with SurgeHealthActorMBean {
  implicit val executionContext: ExecutionContext = ExecutionContext.global

  override def managementName(): String = SurgeHealthActor.managementName

  override def receive: Receive = stopped

  def stopped: Receive = { case StartManagement() =>
    context.become(started(SurgeHealthState(registry = Seq[HealthRegistrationDetail]())))
  }

  def started(state: SurgeHealthState): Receive = {
    case add: AddComponent =>
      context.become(started(state.copy(registry = state.registry ++ Seq(add.detail))))
    case remove: RemoveComponent =>
      context.become(started(state.copy(registry = state.registry.filter(d => d.componentName != remove.componentName))))
    case _: GetHealthRegistry =>
      sender() ! state.registry
      context.become(started(state.copy()))
    case _: StopManagement =>
      context.stop(self)
  }

  def asyncGetHealthRegistry: Future[Seq[HealthRegistrationDetail]] = {
    context.self.ask(GetHealthRegistry())(20.seconds).mapTo[Seq[HealthRegistrationDetail]]
  }

  override def getHealthRegistry: CompositeData = {
    val registryEntries = Await.result(asyncGetHealthRegistry, 30.seconds).map(detail => HealthRegistrationDetailMxView(detail))
    HealthRegistryMxView(registryEntries, self).asCompositeData()
  }

  override def getMXTypeName: String = managementName()

  override def start(componentName: String): Unit = {
    asyncGetHealthRegistry.andThen {
      case Success(data) =>
        data
          .find(d => d.componentName == componentName)
          .foreach(d => {
            context.system.actorSelection(d.controlRefPath).tell(StartComponent(d.componentName, replyTo = self), self)
          })
      case Failure(exception) =>
        log.error("Failed to retrieve Health Registry data", exception)
    }
  }

  override def stop(componentName: String): Unit = {
    asyncGetHealthRegistry.andThen {
      case Success(data) =>
        data
          .find(d => d.componentName == componentName)
          .foreach(d => {
            val selection = context.system.actorSelection(d.controlRefPath)
            selection.resolveOne(10.seconds).andThen {
              case Success(ref)   => ref ! ShutdownComponent(d.componentName, replyTo = self)
              case Failure(error) => log.error("Failed to stop", error)
            }
          })
      case Failure(exception) =>
        log.error("Failed to retrieve Health Registry data", exception)
    }
  }

  override def restart(componentName: String): Unit = {
    asyncGetHealthRegistry.andThen {
      case Success(data) =>
        data
          .find(d => d.componentName == componentName)
          .foreach(d => {
            context.system.actorSelection(d.controlRefPath).tell(RestartComponent(d.componentName, replyTo = self), self)
          })
      case Failure(exception) =>
        log.error("Failed to retrieve Health Registry data", exception)
    }
  }

  override def registeredComponentNames(): util.List[String] = {
    val list = Await.result(asyncGetHealthRegistry, 30.seconds).toList

    val returnedList = new util.ArrayList[String]
    list.foreach(r => returnedList.add(r.componentName))

    returnedList
  }

  override def countRegisteredComponents(): Int = registeredComponentNames().size()

  override def exists(componentName: String): Boolean = {
    Await.result(asyncExists(componentName), TimeoutConfig.HealthSupervision.actorAskTimeout)
  }

  private def asyncExists(componentName: String): Future[Boolean] = {
    asyncGetHealthRegistry
      .andThen { case Success(data) =>
        val found = data.find(d => d.componentName == componentName)
        found.foreach(d => {
          context.system.actorSelection(d.controlRefPath).ask(QueryComponentExists(d.componentName), self)(TimeoutConfig.HealthSupervision.actorAskTimeout)
        })
        found
      }
      .map(d => d.nonEmpty)
  }
}

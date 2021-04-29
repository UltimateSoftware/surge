// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.cluster

import akka.Done
import akka.actor.{ Actor, ActorSystem, PoisonPill, Props }
import akka.testkit.TestKit
import org.scalatest.concurrent.{ Eventually, PatienceConfiguration, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.{ AnyWordSpec, AnyWordSpecLike }
import surge.internal.utils.Logging
import surge.kafka.HostPort

import scala.concurrent.{ ExecutionContext, Future }

class ActorRegistrySpec extends TestKit(ActorSystem("ActorRegistrySpec")) with AnyWordSpecLike
  with Matchers with Eventually with ScalaFutures with PatienceConfiguration {

  private implicit val executionContext: ExecutionContext = ExecutionContext.global

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = Span(10, Seconds), interval = Span(10, Millis))

  class RegisteredActor(registeredKey: String, tags: List[String] = List.empty) extends Actor with ActorRegistrySupport with Logging {
    override val actorSystem: ActorSystem = system
    implicit val executionContext: ExecutionContext = context.dispatcher

    private case object RegisterSelf
    override def receive: Receive = {
      case RegisterSelf => registerSelf()
    }

    private def registerSelf(): Unit = {
      log.debug(s"Registering actor ${self.path}")
      registerService(registeredKey, self, tags).recover {
        case _ => self ! RegisterSelf
      }
    }

    override def preStart(): Unit = {
      registerSelf()
      super.preStart()
    }
  }

  "ActorRegistry" should {
    "Retrieve from inventory an actor recently created" in {
      val key = "TestKey"
      val ref = system.actorOf(Props(new RegisteredActor(key)))
      val registry = new ActorRegistry(system)
      eventually {
        registry.discoverActors(key, List(HostPort("localhost", 0))).futureValue shouldEqual List(ref.path.toString)
      }
    }
    "Retrieve from inventory an actor by tag" in {
      val key = "TagTestKey"
      val ref = system.actorOf(Props(new RegisteredActor(key, List("someTag"))))
      val registry = new ActorRegistry(system)
      eventually {
        registry.discoverActors(key, List(HostPort("localhost", 0)), List("someTag")).futureValue shouldEqual List(ref.path.toString)
        registry.discoverActors(key, List(HostPort("localhost", 0)), List("NonExistingTag")).futureValue shouldBe empty
      }
    }
    "Automatically remove an actor who dies" in {
      val key = "TerminatingActorKey"
      val ref = system.actorOf(Props(new RegisteredActor(key)))
      val registry = new ActorRegistry(system)
      eventually {
        registry.discoverActors(key, List(HostPort("localhost", 0))).futureValue shouldEqual List(ref.path.toString)
      }
      ref ! PoisonPill
      eventually {
        registry.discoverActors(key, List(HostPort("localhost", 0))).futureValue shouldBe empty
      }
    }
  }
}

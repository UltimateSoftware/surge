// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.cluster

import akka.actor.{ Actor, ActorSystem, PoisonPill, Props }
import akka.testkit.TestKit
import com.ultimatesoftware.scala.core.kafka.HostPort
import com.ultimatesoftware.support.Logging
import org.scalatest.concurrent.{ Eventually, PatienceConfiguration, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class ActorRegistrySpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually with PatienceConfiguration {

  override implicit val patienceConfig = PatienceConfig(
    timeout = Span(10, Seconds), interval = Span(10, Millis)) // scalastyle:ignore magic.number

  val system = ActorSystem("ActorRegistrySpec1")
  implicit val executionContext = system.dispatcher

  trait TestActorRegistry extends ActorRegistry {
    override val actorSystem = system
  }

  class RegisteredActor extends Actor with TestActorRegistry with Logging {
    implicit val executionContext: ExecutionContext = context.dispatcher

    override def receive: Receive = {
      case _ ⇒ // ignore, this actor does nothing
    }

    override def preStart(): Unit = {
      log.debug(s"Registering actor ${self.path}")
      registerService("TestKey", self, List("tag1", "tag2"))
      super.preStart()
    }
  }

  "ActorRegistry" should {
    val ref = system.actorOf(Props(new RegisteredActor))
    "Retrieve from inventory an actor recently created" in new TestActorRegistry {
      eventually {
        val discoveredActors = discoverActors("TestKey", List(HostPort("localhost", 0))).futureValue
        discoveredActors shouldEqual List(ref.path.toString)
      }
    }
    "Retrieve from inventory an actor by tag" in new TestActorRegistry {
      val discoveredActors = discoverActors("TestKey", List(HostPort("localhost", 0)), List("NonExistingTag")).futureValue
      discoveredActors shouldBe empty
    }
    "Automatically remove an actor who dies" in new TestActorRegistry {
      ref ! PoisonPill
      eventually {
        val discoveredActors = discoverActors("TestKey", List(HostPort("localhost", 0)), List("NonExistingTag")).futureValue
        discoveredActors shouldBe empty
      }
    }
  }
}

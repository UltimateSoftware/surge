// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.actor

import akka.actor.{ Actor, ActorNotFound, ActorPath, ActorSystem, Props }
import akka.testkit.{ TestKit, TestProbe }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import surge.core.Ack

import scala.concurrent.duration._

class ActorLifecycleManagerActorSpec
    extends TestKit(ActorSystem("actorLifecycleManagerActorSpec"))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with Eventually {
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(30, Seconds)), interval = scaled(Span(10, Milliseconds)))

  case object StopIt
  case class Stopped()

  "ActorLifecycleManagerActor" should {
    "start an actor" in {
      val probe = TestProbe()
      val managed = ActorLifecycleManagerActor.manage(
        system,
        Props(new Actor() {
          override def receive: Receive = { case StopIt =>
            probe.ref ! Stopped()
            context.stop(self)
          }
        }),
        managedActorName = Some("testActor"),
        stopMessageAdapter = Some(() => StopIt),
        componentName = "testComponent")

      managed.start().futureValue shouldEqual Ack
    }

    "gracefully stop actor" in {
      val probe = TestProbe()
      val managed = ActorLifecycleManagerActor.manage(
        system,
        Props(new Actor() {
          override def receive: Receive = { case StopIt =>
            probe.ref ! Stopped()
            context.stop(self)
          }
        }),
        managedActorName = Some("testActor"),
        stopMessageAdapter = Some(() => StopIt),
        componentName = "testComponent")

      managed.start()

      val managerActorPath: Option[ActorPath] = managed.managedPath().futureValue
      managerActorPath shouldBe defined
      managed.stop()

      probe.expectMsg(Stopped())

      // Should be unable to resolve actorSelection after testActor stopped
      eventually {
        val thrown = the[Throwable] thrownBy system.actorSelection(managerActorPath.get).resolveOne()(5.seconds).futureValue
        thrown.getCause shouldBe an[ActorNotFound]
      }
    }
  }
}

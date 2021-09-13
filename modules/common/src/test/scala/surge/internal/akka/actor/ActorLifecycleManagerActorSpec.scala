// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.actor

import akka.actor.{ Actor, ActorNotFound, ActorPath, ActorSystem, Props }
import akka.pattern.ask
import akka.testkit.TestKit
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import surge.internal.akka.actor.ActorLifecycleManagerActor.GetManagedActorPath

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
  "ActorLifecycleManagerActor" should {
    "gracefully stop actor" in {
      val actorRef = system.actorOf(
        Props(
          new ActorLifecycleManagerActor(
            Props(new Actor() {
              override def receive: Receive = { case StopIt =>
                context.stop(self)
              }
            }),
            managedActorName = Some("testActor"),
            stopMessageAdapter = Some(() => StopIt),
            componentName = "testComponent")))

      actorRef ! ActorLifecycleManagerActor.Start

      val managerActorPath: ActorPath = actorRef.ask(GetManagedActorPath)(30.seconds).mapTo[ActorPath].futureValue

      actorRef ! ActorLifecycleManagerActor.Stop

      // Should be unable to resolve actorSelection after testActor stopped
      eventually {
        val thrown = the[Throwable] thrownBy system.actorSelection(managerActorPath).resolveOne()(5.seconds).futureValue
        thrown.getCause shouldBe an[ActorNotFound]
      }
    }
  }
}

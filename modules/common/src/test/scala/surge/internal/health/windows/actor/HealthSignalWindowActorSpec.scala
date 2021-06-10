// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.actor

import akka.actor.ActorSystem
import akka.testkit.{ TestActorRef, TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import surge.health.domain.HealthSignal
import surge.health.windows.{ AddedToWindow, WindowAdvanced, WindowClosed, WindowOpened }
import surge.internal.health.windows.WindowSlider
import surge.internal.health.{ HealthSignalBus, HealthSignalBusInternal }

import scala.concurrent.duration._
import scala.languageFeature.postfixOps
class HealthSignalWindowActorSpec
    extends TestKit(ActorSystem("HealthSignalWindowActorSpec", ConfigFactory.load("artery-test-config")))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with Eventually {
  import surge.internal.health.context.TestContext._

  implicit val postOp: postfixOps = postfixOps

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(160, Seconds)), interval = scaled(Span(5, Seconds)))

  val bus: HealthSignalBusInternal = HealthSignalBus(testHealthSignalStreamProvider(Seq.empty))

  override def afterAll(): Unit = {
    system.classicSystem.terminate()
  }

  "HealthSignalWindowActor" should {
    "tick" in {
      var tickCount: Int = 0
      // override handleTick to track times tick was received
      val actorRef = TestActorRef(new HealthSignalWindowActor(frequency = FiniteDuration(5, "seconds"), WindowSlider(1, 0)) {
        override def handleTick(state: WindowState): Unit = {
          tickCount += 1
        }
      })

      val probe = TestProbe()
      probe.watch(actorRef)

      // WindowActorRef that will tick every 1 second
      val windowRef =
        new HealthSignalWindowActorRef(actor = actorRef, windowFreq = FiniteDuration(1, "second"), filters = Seq.empty, signalBus = bus, actorSystem = system)
          .start(Some(probe.ref))

      eventually {
        // HealthSignalWindowActor should handleTick 5 times; 1 tick per second
        tickCount shouldEqual 5
      }

      windowRef.stop()

      eventually {
        probe.expectTerminated(actorRef)
      }
    }

    "when sliding configured; advance on Window Expired" in {
      val actorRef: HealthSignalWindowActorRef = HealthSignalWindowActor(system, FiniteDuration(5, "seconds"), WindowSlider(1, 0), Seq.empty, bus)
      val probe = TestProbe()
      probe.watch(actorRef.actor)

      actorRef.start(Some(probe.ref))

      // Open Window
      val openedWindow = probe.fishForMessage(max = FiniteDuration(1, "second")) { case _: WindowOpened =>
        true
      }

      Option(openedWindow).nonEmpty shouldEqual true
      val maybeWindow = openedWindow.asInstanceOf[WindowOpened].window()
      maybeWindow.nonEmpty shouldEqual true

      // Wait for window to expire
      eventually {
        val advancedWindow = probe.fishForMessage(max = FiniteDuration(1, "second")) { case _: WindowAdvanced =>
          true
        }

        val windowExpired = Option(advancedWindow).exists(adv =>
          adv
            .asInstanceOf[WindowAdvanced]
            .window()
            .exists(w => {
              w.expired()
            }))
        windowExpired shouldEqual true
      }

      actorRef.stop()

      eventually {
        probe.expectTerminated(actorRef.actor)
      }
    }

    // fixme: broken test after merge
    "when sliding configured with no buffer; advance on AddedToWindow" ignore {
      val actorRef = HealthSignalWindowActor(system, FiniteDuration(5, "seconds"), WindowSlider(1, 0), Seq.empty, bus)
      val probe = TestProbe()
      probe.watch(actorRef.actor)
      actorRef.start(Some(probe.ref))

      // Open Window
      probe.expectMsgClass(classOf[WindowOpened])

      actorRef.processSignal(mock(classOf[HealthSignal]))

      probe.expectMsgClass(classOf[AddedToWindow])

      probe.expectMsgClass(FiniteDuration(10, "seconds"), classOf[WindowAdvanced])

      actorRef.stop()

      eventually {
        probe.expectTerminated(actorRef.actor)
      }
    }

    // fixme: broken test after merge
    "when sliding configured with buffer; advance on CloseWindow" ignore {
      val actorRef = HealthSignalWindowActor(system, FiniteDuration(5, "seconds"), WindowSlider(1), Seq.empty, bus)
      val probe = TestProbe()
      probe.watch(actorRef.actor)

      actorRef.start(Some(probe.ref))

      // Open Window
      probe.expectMsgClass(classOf[WindowOpened])

      actorRef.processSignal(mock(classOf[HealthSignal]))

      actorRef.closeWindow()

      probe.expectMsgClass(classOf[AddedToWindow])

      probe.expectMsgClass(max = FiniteDuration(10, "seconds"), classOf[WindowClosed])
      probe.expectMsgClass(max = FiniteDuration(10, "seconds"), classOf[WindowAdvanced])

      actorRef.stop()

      eventually {
        probe.expectTerminated(actorRef.actor)
      }
    }
  }
}

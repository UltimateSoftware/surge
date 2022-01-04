// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.actor

import akka.actor.ActorSystem
import akka.testkit.{ TestActorRef, TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import surge.health.HealthSignalBusTrait
import surge.health.domain.HealthSignal
import surge.health.matchers.SignalPatternMatcherDefinition.SignalNameEqualsMatcherDefinition
import surge.health.windows._
import surge.internal.health.windows.WindowSlider
import surge.internal.health.{ HealthSignalBus, HealthSignalBusInternal }
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._

class HealthSignalWindowActorSpec
    extends TestKit(ActorSystem("HealthSignalWindowActorSpec", ConfigFactory.load("artery-test-config")))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with Eventually {
  import surge.internal.health.context.TestContext._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(10, Milliseconds)))

  val bus: HealthSignalBusInternal = HealthSignalBus(testHealthSignalStreamProvider(Seq.empty))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  "HealthSignalWindowActor" should {
    "tick" in {
      var tickCount: Int = 0
      // override handleTick to track times tick was received
      val actorRef = TestActorRef(
        new HealthSignalWindowActor(
          frequency = 5.seconds,
          resumeWindowProcessingDelay = 10.milliseconds,
          signalBus = Mockito.mock(classOf[HealthSignalBusTrait]),
          signalPatternMatcherDefinition = SignalNameEqualsMatcherDefinition("place-holder-matcher-for-test", 10.seconds, None),
          windowAdvanceStrategy = WindowSlider(1, 0)) {
          override def tick(state: WindowState): WindowState = {
            tickCount += 1
            state
          }
        })

      val probe = TestProbe()
      probe.watch(actorRef)

      // WindowActorRef that will tick every 10 millis
      val windowRef =
        new HealthSignalWindowActorRef(
          actor = actorRef,
          windowFreq = 1.second,
          initialWindowProcessingDelay = 10.milliseconds,
          tickInterval = 10.milliseconds,
          actorSystem = system).start(Some(probe.ref))

      eventually {
        // HealthSignalWindowActor should handleTick 5 times; 1 tick per second
        tickCount shouldBe >=(right = 5)
      }

      windowRef.stop()

      eventually {
        probe.expectTerminated(actorRef)
      }
    }

    "get window snapshot" in {
      val actorRef: HealthSignalWindowActorRef =
        HealthSignalWindowActor(
          actorSystem = system,
          windowFrequency = 100.seconds,
          initialWindowProcessingDelay = 10.milliseconds,
          resumeWindowProcessingDelay = 10.milliseconds,
          advancer = WindowSlider(1, 0),
          signalBus = Mockito.mock(classOf[HealthSignalBusTrait]),
          signalPatternMatcherDefinition = SignalNameEqualsMatcherDefinition("place-holder-matcher-for-test", 10.seconds, None),
          windowCheckInterval = 10.milliseconds)
      val probe = TestProbe()
      probe.watch(actorRef.actor)

      actorRef.start(Some(probe.ref))

      probe.expectMsgClass(10.seconds, classOf[WindowOpened])

      val mockHealthSignal = mock[HealthSignal](classOf[HealthSignal])
      actorRef.processSignal(mockHealthSignal)

      probe.expectMsgClass(10.seconds, classOf[AddedToWindow])

      val snapshot = eventually {
        val ready = actorRef.windowSnapshot().futureValue
        ready shouldBe defined
        ready.get.data.nonEmpty shouldEqual true
        ready.get
      }

      snapshot.data.head shouldEqual mockHealthSignal
      actorRef.stop()

      eventually {
        probe.expectTerminated(actorRef.actor)
      }
    }

    "ignore signals when paused" in {
      val actorRef: HealthSignalWindowActorRef =
        HealthSignalWindowActor(
          actorSystem = system,
          windowFrequency = 100.seconds,
          initialWindowProcessingDelay = 10.milliseconds,
          resumeWindowProcessingDelay = 10.milliseconds,
          advancer = WindowSlider(1, 0),
          signalBus = Mockito.mock(classOf[HealthSignalBusTrait]),
          signalPatternMatcherDefinition = SignalNameEqualsMatcherDefinition("place-holder-matcher-for-test", 10.seconds, None),
          windowCheckInterval = 10.milliseconds)
      val probe = TestProbe()
      probe.watch(actorRef.actor)

      actorRef.start(Some(probe.ref))

      eventually {
        val opened = probe.fishForMessage(100.milliseconds) { msg =>
          msg.isInstanceOf[WindowOpened]
        }

        opened shouldBe a[WindowOpened]
      }

      actorRef.processSignal(mock(classOf[HealthSignal]))
      probe.expectMsgClass(10.seconds, classOf[AddedToWindow])

      actorRef.pause(10.seconds)
      actorRef.processSignal(mock(classOf[HealthSignal]))

      whenReady(actorRef.windowSnapshot()) { maybeSnapshot =>
        maybeSnapshot shouldBe defined
        maybeSnapshot.get.data.size shouldEqual 1
      }

      actorRef.stop()

      eventually {
        probe.expectTerminated(actorRef.actor)
      }
    }

    "pause and resume after flush" in {
      val actorRef: HealthSignalWindowActorRef =
        HealthSignalWindowActor(
          actorSystem = system,
          windowFrequency = 100.milliseconds,
          initialWindowProcessingDelay = 10.milliseconds,
          resumeWindowProcessingDelay = 10.milliseconds,
          advancer = WindowSlider(1, 0),
          signalBus = Mockito.mock(classOf[HealthSignalBusTrait]),
          signalPatternMatcherDefinition = SignalNameEqualsMatcherDefinition("place-holder-matcher-for-test", 10.seconds, None),
          windowCheckInterval = 10.milliseconds)
      val probe = TestProbe()
      probe.watch(actorRef.actor)

      actorRef.start(Some(probe.ref))

      actorRef.flush()

      eventually {
        probe.expectMsgType[WindowPaused](1.second)
      }

      eventually {
        probe.expectMsgType[WindowResumed](1.second)
      }

      actorRef.stop()

      eventually {
        probe.expectTerminated(actorRef.actor)
      }
    }

    "when sliding configured; advance on Window Expired" in {
      val actorRef: HealthSignalWindowActorRef =
        HealthSignalWindowActor(
          actorSystem = system,
          windowFrequency = 100.milliseconds,
          initialWindowProcessingDelay = 10.milliseconds,
          resumeWindowProcessingDelay = 10.milliseconds,
          advancer = WindowSlider(1, 0),
          signalPatternMatcherDefinition = SignalNameEqualsMatcherDefinition("place-holder-matcher-for-test", 10.seconds, None),
          signalBus = Mockito.mock(classOf[HealthSignalBusTrait]),
          windowCheckInterval = 10.milliseconds)
      val probe = TestProbe()
      probe.watch(actorRef.actor)

      actorRef.start(Some(probe.ref))

      // Open Window
      val openedWindow = probe.fishForMessage(max = 1.second) { case _: WindowOpened =>
        true
      }

      Option(openedWindow).nonEmpty shouldEqual true
      val maybeWindow = openedWindow.asInstanceOf[WindowOpened].window()
      maybeWindow.nonEmpty shouldEqual true

      // Wait for window to expire
      val closedWindow = eventually {
        probe.expectMsgType[WindowClosed]
      }
      closedWindow.window() shouldBe defined
      closedWindow.window().get.expired() shouldEqual true

      val advancedWindow = probe.expectMsgType[WindowAdvanced]
      advancedWindow.window() shouldBe defined
      advancedWindow.window().get.expired() shouldEqual false

      actorRef.stop()

      eventually {
        probe.expectTerminated(actorRef.actor)
      }
    }

    "when sliding configured with no buffer; advance on AddedToWindow" in {
      val actorRef =
        HealthSignalWindowActor(
          actorSystem = system,
          initialWindowProcessingDelay = 10.milliseconds,
          resumeWindowProcessingDelay = 10.milliseconds,
          windowFrequency = 100.milliseconds,
          advancer = WindowSlider(1, 0),
          signalPatternMatcherDefinition = SignalNameEqualsMatcherDefinition("place-holder-matcher-for-test", 10.seconds, None),
          signalBus = Mockito.mock(classOf[HealthSignalBusTrait]),
          windowCheckInterval = 10.milliseconds)
      val probe = TestProbe()
      probe.watch(actorRef.actor)
      actorRef.start(Some(probe.ref))

      // Open Window
      probe.expectMsgClass(classOf[WindowOpened])

      actorRef.processSignal(mock(classOf[HealthSignal]))

      val maybeAddedEvent = probe.fishForMessage(10.seconds) { case msg =>
        msg.isInstanceOf[AddedToWindow]
      }

      maybeAddedEvent shouldBe a[AddedToWindow]

      try {
        probe.expectMsgClass(15.seconds, classOf[WindowAdvanced])
      } finally {
        actorRef.stop()
      }

      eventually {
        probe.expectTerminated(actorRef.actor)
      }
    }

    "when sliding configured with buffer; advance on CloseWindow" in {
      val actorRef =
        HealthSignalWindowActor(
          actorSystem = system,
          initialWindowProcessingDelay = 10.milliseconds,
          resumeWindowProcessingDelay = 10.milliseconds,
          windowFrequency = 100.milliseconds,
          advancer = WindowSlider(1, 10),
          signalPatternMatcherDefinition = SignalNameEqualsMatcherDefinition("place-holder-matcher-for-test", 10.seconds, None),
          signalBus = Mockito.mock(classOf[HealthSignalBusTrait]),
          windowCheckInterval = 10.milliseconds)

      val probe = TestProbe()
      probe.watch(actorRef.actor)

      actorRef.start(Some(probe.ref))

      // Open Window
      probe.expectMsgClass(classOf[WindowOpened])

      actorRef.processSignal(mock(classOf[HealthSignal]))

      actorRef.closeWindow()

      val maybeAddedEvent = probe.fishForMessage(10.seconds) { case msg =>
        msg.isInstanceOf[AddedToWindow]
      }

      maybeAddedEvent shouldBe a[AddedToWindow]

      probe.expectMsgType[WindowClosed]
      val advanced = probe.expectMsgType[WindowAdvanced]

      (advanced.d.signals should have).length(1)

      actorRef.stop()

      eventually {
        probe.expectTerminated(actorRef.actor)
      }
    }
  }
}

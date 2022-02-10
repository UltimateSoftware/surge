// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.health.config.{ ThrottleConfig, WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ HealthSignal, Trace }
import surge.health.matchers.SignalPatternMatcherDefinition.{ RepeatingSignalMatcherDefinition, SignalNameEqualsMatcherDefinition }
import surge.health.matchers.{ SideEffect, SignalPatternMatcherDefinition }
import surge.health.windows._
import surge.health.{ HealthSignalStream, SignalType }
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import scala.concurrent.ExecutionContext.Implicits.global

import java.util.regex.Pattern
import scala.concurrent.duration._
class HealthSignalStreamProviderSpec extends TestKit(ActorSystem("HealthSignalStreamProviderSpec")) with AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  import surge.internal.health.context.TestContext._

  "HealthSignalStreamProvider" should {
    import org.mockito.Mockito._
    "have filters from ctor" in {
      val initialFilters = Seq(SignalPatternMatcherDefinition.nameEquals(signalName = "foo", 10.seconds))
      val provider = testHealthSignalStreamProvider(initialFilters)
      provider.patternMatchers() shouldEqual initialFilters
    }

    "return bus" in {
      val signal = HealthSignal(topic = "topic", name = "5 in a row", signalType = SignalType.TRACE, data = Trace("test"), source = None)
      val initialFilters: Seq[SignalPatternMatcherDefinition] = Seq(
        SignalPatternMatcherDefinition.nameEquals(signalName = "foo", 10.seconds),
        SignalPatternMatcherDefinition.repeating(times = 5, pattern = Pattern.compile("bar"), 10.seconds, Some(SideEffect(Seq(signal)))))
      val provider = testHealthSignalStreamProvider(initialFilters)
      val bus = provider.bus()

      bus shouldBe a[HealthSignalBusInternal]

      provider.patternMatchers() shouldEqual initialFilters

      val streamRef: HealthSignalStream = bus.signalStream()

      streamRef.patternMatchers().size shouldEqual 2
      streamRef.patternMatchers().exists(p => p.isInstanceOf[SignalNameEqualsMatcherDefinition]) shouldEqual true
      streamRef.patternMatchers().exists(p => p.isInstanceOf[RepeatingSignalMatcherDefinition]) shouldEqual true

      streamRef.unsubscribe().stop()

      bus.unsupervise()
    }

    "return filters" in {
      val initialFilters = Seq(mock(classOf[SignalPatternMatcherDefinition]))
      val provider = testHealthSignalStreamProvider(initialFilters)

      provider.patternMatchers() shouldEqual initialFilters
    }
  }

  "SlidingHealthSignalProvider" should {
    "provide a signalBus with Supervision" in {
      val initialFilters = Seq(SignalPatternMatcherDefinition.nameEquals(signalName = "foo", 10.seconds))

      val provider = new SlidingHealthSignalStreamProvider(
        WindowingStreamConfig(
          advancerConfig = WindowingStreamSliderConfig(buffer = 10, advanceAmount = 1),
          throttleConfig = ThrottleConfig(elements = 100, duration = 5.seconds),
          windowingInitDelay = 5.seconds,
          windowingResumeDelay = 5.seconds,
          maxWindowSize = 500),
        system,
        streamMonitoring = None,
        initialFilters)

      val supervisedBus = provider.bus()

      // HealthSupervisorRef should be available
      supervisedBus.supervisor().isDefined shouldEqual true
      supervisedBus.signalStream().unsubscribe().stop()

      supervisedBus.unsupervise()
    }
  }

  "StreamMonitoringRef" should {
    import org.mockito.Mockito._
    "forward messages to actorRef" in {
      val probe = TestProbe()
      val ref = new StreamMonitoringRef(probe.ref)

      ref.windowOpened(mock(classOf[Window]))
      ref.dataAddedToWindow(mock(classOf[HealthSignal]), mock(classOf[Window]))
      ref.windowClosed(mock(classOf[Window]), Seq.empty)
      ref.windowAdvanced(mock(classOf[Window]), Seq.empty)
      ref.windowStopped(mock(classOf[Option[Window]]))

      // Expect messages
      probe.expectMsgClass(classOf[WindowOpened])
      probe.expectMsgClass(classOf[AddedToWindow])
      probe.expectMsgClass(classOf[WindowClosed])
      probe.expectMsgClass(classOf[WindowAdvanced])
      probe.expectMsgClass(classOf[WindowStopped])
    }
  }
}

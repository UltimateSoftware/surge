// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.health.config.{ WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ HealthSignal, Trace }
import surge.health.matchers.{ SideEffect, SignalPatternMatcher }
import surge.health.windows.{ AddedToWindow, Window, WindowAdvanced, WindowClosed, WindowOpened, WindowStopped }
import surge.health.{ HealthSignalStream, SignalType }
import surge.internal.health.matchers.{ RepeatingSignalMatcher, SignalNameEqualsMatcher }
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider

import scala.languageFeature.postfixOps

class HealthSignalStreamProviderSpec extends TestKit(ActorSystem("HealthSignalStreamProviderSpec")) with AnyWordSpecLike with Matchers {
  import surge.internal.health.context.TestContext._
  implicit val postOp: postfixOps = postfixOps

  "HealthSignalStreamProvider" should {
    import org.mockito.Mockito._
    "have filters from ctor" in {
      val initialFilters = Seq(SignalNameEqualsMatcher(name = "foo"))
      val provider = testHealthSignalStreamProvider(initialFilters)
      provider.filters() shouldEqual initialFilters
    }

    "return bus" in {
      val signal = HealthSignal(topic = "topic", name = "5 in a row", signalType = SignalType.TRACE, data = Trace("test"))
      val initialFilters: Seq[SignalPatternMatcher] = Seq(
        SignalNameEqualsMatcher(name = "foo"),
        RepeatingSignalMatcher(times = 5, atomicMatcher = SignalNameEqualsMatcher(name = "bar"), Some(SideEffect(Seq(signal)))))
      val provider = testHealthSignalStreamProvider(initialFilters)
      val bus = provider.busWithSupervision()

      bus shouldBe a[HealthSignalBusInternal]
      bus shouldEqual provider.busWithSupervision()

      provider.filters() shouldEqual initialFilters

      val streamRef: HealthSignalStream = bus.signalStream()

      streamRef.filters().size shouldEqual 2
      streamRef.filters().exists(p => p.isInstanceOf[SignalNameEqualsMatcher]) shouldEqual true
      streamRef.filters().exists(p => p.isInstanceOf[RepeatingSignalMatcher]) shouldEqual true

      streamRef.unsubscribe().stop()

      bus.unsupervise()
    }

    "return filters" in {
      val initialFilters = Seq(mock(classOf[SignalPatternMatcher]))
      val provider = testHealthSignalStreamProvider(initialFilters)

      provider.filters() shouldEqual initialFilters
    }
  }

  "SlidingHealthSignalProvider" should {
    "provide a signalBus with Supervision" in {
      val initialFilters = Seq(SignalNameEqualsMatcher(name = "foo"))

      val provider = new SlidingHealthSignalStreamProvider(
        WindowingStreamConfig(advancerConfig = WindowingStreamSliderConfig()),
        system,
        streamMonitoring = None,
        initialFilters)

      val supervisedBus = provider.busWithSupervision()

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

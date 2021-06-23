// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.supervisor

import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import org.mockito.{ ArgumentMatchers, Mockito }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import surge.core.ControllableWithHooks
import surge.health.config.{ WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ HealthSignal, Trace }
import surge.health.matchers.SideEffect
import surge.health.{ Ack, HealthSupervisorTrait, SignalType }
import surge.internal.health._
import surge.internal.health.matchers.SignalNameEqualsMatcher
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider

import scala.concurrent.Future
import scala.concurrent.duration._

class HealthSupervisorActorSpec
    extends TestKit(ActorSystem("HealthSignalSupervisorSpec"))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with Eventually {
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(160, Seconds)), interval = scaled(Span(5, Seconds)))

  private val testHealthSignal = HealthSignal(topic = "health.signal", name = "boom", signalType = SignalType.TRACE, data = Trace("test"))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "HealthSupervisorActorSpec" should {
    "sliding stream; attempt to restart registered actor" in {
      import org.mockito.Mockito._

      val probe = TestProbe()

      val bus: HealthSignalBusInternal = new SlidingHealthSignalStreamProvider(
        WindowingStreamConfig(advancerConfig = WindowingStreamSliderConfig()),
        system,
        streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
        Seq(SignalNameEqualsMatcher(name = "test.trace", Some(SideEffect(Seq(testHealthSignal)))))).busWithSupervision()

      // Start signal streaming
      bus.signalStream().start()

      // Register
      val controllable: ControllableWithHooks = mock(classOf[ControllableWithHooks])
      Mockito.when(controllable.restart()).thenCallRealMethod()
      val done = bus.register(controllable, componentName = "boomControl", Seq(Pattern.compile("boom")))
      done shouldBe a[Future[_]]
      val received = probe.receiveN(1, 10.seconds)
      Option(received).nonEmpty shouldEqual true

      // Signal
      bus.signalWithTrace(name = "test.trace", Trace("test trace")).emit()

      eventually {
        verify(controllable, times(1)).restart()
      }

      bus.unsupervise().signalStream().stop()
    }

    "receive registration" in {
      import org.mockito.Mockito._

      val probe = TestProbe()

      val bus = new SlidingHealthSignalStreamProvider(
        WindowingStreamConfig(advancerConfig = WindowingStreamSliderConfig()),
        system,
        streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
        Seq(SignalNameEqualsMatcher(name = "test.trace", Some(SideEffect(Seq(testHealthSignal)))))).busWithSupervision()
      val ref: HealthSupervisorTrait = bus.supervisor().get

      val controllable: ControllableWithHooks = mock(classOf[ControllableWithHooks])
      Mockito.when(controllable.restart()).thenCallRealMethod()
      val message = bus.registration(controllable, componentName = "boomControl", Seq.empty)
      val done = ref.register(message.underlyingRegistration())
      done shouldBe a[Future[_]]

      Mockito.verify(controllable, times(1)).onShutdown(ArgumentMatchers.any())
      Mockito.verify(controllable, times(1)).onRestart(ArgumentMatchers.any())

      val received = probe.receiveN(1, max = 10.seconds)

      received.headOption.nonEmpty shouldEqual true
      received.head.isInstanceOf[HealthRegistrationReceived]

      received.head.asInstanceOf[HealthRegistrationReceived].registration shouldEqual message.underlyingRegistration()

      message.underlyingRegistration().control shouldBe a[ControllableWithHooks]

      ref.stop()
    }

    "receive signal" in {
      val probe: TestProbe = TestProbe()
      val bus = new SlidingHealthSignalStreamProvider(
        WindowingStreamConfig(advancerConfig = WindowingStreamSliderConfig()),
        system,
        streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
        Seq(SignalNameEqualsMatcher(name = "test.trace", Some(SideEffect(Seq(testHealthSignal)))))).busWithSupervision()
      val ref: HealthSupervisorTrait = bus.supervisor().get

      bus.signalStream().start()
      val message = bus.signalWithTrace(name = "test", Trace("test trace"))
      message.emit()

      val received = probe.fishForMessage(max = 1.second) { case msg =>
        msg.isInstanceOf[HealthSignalReceived]
      }

      received.asInstanceOf[HealthSignalReceived].signal shouldEqual message.underlyingSignal()

      ref.stop()
    }
  }

  "HealthSignalStreamMonitoringRefWithSupervisionSupport" should {
    import org.mockito.Mockito._
    "proxy to actorRef" in {
      val probe = TestProbe()
      val monitor = new HealthSignalStreamMonitoringRefWithSupervisionSupport(actor = probe.ref)

      monitor.registrationReceived(mock(classOf[HealthRegistrationReceived]))
      monitor.healthSignalReceived(mock(classOf[HealthSignalReceived]))

      probe.expectMsgClass(classOf[HealthRegistrationReceived])
      probe.expectMsgClass(classOf[HealthSignalReceived])
    }
  }
}

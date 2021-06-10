// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.supervisor

import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import surge.health.HealthSupervisorTrait
import surge.health.config.{ WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ HealthSignalBuilder, Trace }
import surge.health.matchers.SideEffect
import surge.internal.health.matchers.SignalNameEqualsMatcher
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import surge.internal.health._

import scala.concurrent.duration._

class HealthSupervisorActorSpec extends TestKit(ActorSystem("HealthSignalSupervisorSpec")) with AnyWordSpecLike with Matchers with Eventually {
  implicit val postfixOps: languageFeature.postfixOps = scala.language.postfixOps
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(160, Seconds)), interval = scaled(Span(5, Seconds)))

  "HealthSupervisorActorSpec" should {
    "sliding stream; attempt to restart registered actor" in {
      val probe = TestProbe()

      val bus: HealthSignalBusInternal = new SlidingHealthSignalStreamProvider(
        WindowingStreamConfig(advancerConfig = WindowingStreamSliderConfig()),
        system,
        streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
        Seq(SignalNameEqualsMatcher(name = "test.trace", Some(SideEffect(Seq(HealthSignalBuilder("health.signal").withName(name = "boom").build()))))))
        .busWithSupervision()

      // Start signal streaming
      bus.signalStream().start()

      // Register
      bus.registration(probe.ref, componentName = "boomControl", Seq(Pattern.compile("boom"))).invoke()

      val received = probe.receiveN(1, 10.seconds)
      Option(received).nonEmpty shouldEqual true

      // Signal
      bus.signalWithTrace(name = "test.trace", Trace("test trace")).emit()

      eventually {
        // Verify restart
        val restart = probe.fishForMessage(max = 100 millis) { case msg =>
          msg.isInstanceOf[RestartComponent]
        }
        Option(restart.asInstanceOf[RestartComponent].replyTo).isDefined shouldEqual true
      }

      bus.unsupervise().signalStream().stop()
    }

    "receive registration" in {
      val probe = TestProbe()

      val bus = new SlidingHealthSignalStreamProvider(
        WindowingStreamConfig(advancerConfig = WindowingStreamSliderConfig()),
        system,
        streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
        Seq(SignalNameEqualsMatcher(name = "test.trace", Some(SideEffect(Seq(HealthSignalBuilder("health.signal").withName(name = "boom").build()))))))
        .busWithSupervision()
      val ref: HealthSupervisorTrait = bus.supervisor().get

      val message = bus.registration(probe.ref, componentName = "boomControl", Seq.empty)
      ref.register(message.underlyingRegistration())

      val received = probe.receiveN(1, max = 10 seconds)

      received.headOption.nonEmpty shouldEqual true
      received.head.isInstanceOf[HealthRegistrationReceived]

      received.head.asInstanceOf[HealthRegistrationReceived].registration shouldEqual message.underlyingRegistration()

      ref.stop()
    }

    "receive signal" in {
      val probe: TestProbe = TestProbe()
      val bus = new SlidingHealthSignalStreamProvider(
        WindowingStreamConfig(advancerConfig = WindowingStreamSliderConfig()),
        system,
        streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
        Seq(SignalNameEqualsMatcher(name = "test.trace", Some(SideEffect(Seq(HealthSignalBuilder("health.signal").withName(name = "boom").build()))))))
        .busWithSupervision()
      val ref: HealthSupervisorTrait = bus.supervisor().get

      bus.signalStream().start()
      val message = bus.signalWithTrace(name = "test", Trace("test trace"))
      message.emit()

      val received = probe.fishForMessage(max = 1 second) { case msg =>
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

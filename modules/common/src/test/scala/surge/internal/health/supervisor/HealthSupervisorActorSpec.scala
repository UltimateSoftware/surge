// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.supervisor

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import surge.core.{ Ack, ControllableAdapter }
import surge.health.config.{ ThrottleConfig, WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ HealthSignal, Trace }
import surge.health.matchers.SideEffect
import surge.health._
import surge.internal.health._
import surge.internal.health.matchers.SignalNameEqualsMatcher
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider

import java.util.regex.Pattern
import scala.concurrent.Future
import scala.concurrent.duration._

class HealthSupervisorActorSpec
    extends TestKit(ActorSystem("HealthSignalSupervisorSpec"))
    with AnyWordSpecLike
    with ScalaFutures
    with BeforeAndAfterAll
    with Matchers
    with Eventually {
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(10, Milliseconds)))

  private val testHealthSignal = HealthSignal(topic = "health.signal", name = "boom", signalType = SignalType.TRACE, data = Trace("test"))

  case class TestContext(probe: TestProbe, bus: HealthSignalBusTrait)

  def testContext[T](testFun: TestContext => T): T = {
    val probe = TestProbe()
    val bus = new SlidingHealthSignalStreamProvider(
      WindowingStreamConfig(
        advancerConfig = WindowingStreamSliderConfig(buffer = 10, advanceAmount = 1),
        throttleConfig = ThrottleConfig(elements = 100, duration = 5.seconds),
        windowingDelay = 5.seconds,
        maxWindowSize = 500,
        frequencies = Seq(10.seconds)),
      system,
      streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
      Seq(SignalNameEqualsMatcher(name = "test.trace", Some(SideEffect(Seq(testHealthSignal)))))).bus()
    try {
      testFun(TestContext(probe, bus))
    } finally {
      bus.signalStream().stop()
    }
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  "HealthSupervisorActorSpec" should {
    "sliding stream; attempt to restart registered actor" in testContext { ctx =>
      import ctx._
      // Start signal streaming
      bus.signalStream().start()

      val restartProbe = TestProbe()
      val control = new ControllableAdapter() {
        override def restart(): Future[Ack] = Future {
          restartProbe.ref ! RestartComponent("component", probe.ref)
          Ack()
        }(system.dispatcher)
      }

      // Register
      val componentName = "boomControl"
      whenReady(bus.register(control, componentName = componentName, Seq(Pattern.compile("boom")))) { done =>
        done shouldBe a[Ack]
        probe.receiveN(1)

        // Signal
        bus.signalWithTrace(name = "test.trace", Trace("test trace")).emit()

        eventually {
          // Verify restart
          val restart = restartProbe.expectMsgType[RestartComponent]
          restart.name shouldEqual componentName
        }
      }
    }

    "receive registration" in testContext { ctx =>
      import ctx._
      val ref: HealthSupervisorTrait = bus.supervisor().get

      val control = new ControllableAdapter()
      val message = bus.registration(control, componentName = "boomControl", Seq.empty)

      whenReady(ref.register(message.underlyingRegistration())) { done =>
        done shouldBe a[Ack]
      }

      eventually {
        val received = probe.receiveN(1, max = 10.seconds)

        received.headOption.nonEmpty shouldEqual true
        received.head shouldBe a[HealthRegistrationReceived]

        received.head.asInstanceOf[HealthRegistrationReceived].registration.componentName shouldEqual message.underlyingRegistration().componentName

        ref.registrationLinks().exists(l => l.componentName == "boomControl")
      }
      ref.stop()
    }

    "unregister control when it stops" in testContext { ctx =>
      import ctx._
      val ref: HealthSupervisorTrait = bus.supervisor().get

      val control = new ControllableAdapter()
      val message = bus.registration(control, componentName = "boomControl", Seq.empty)

      whenReady(ref.register(message.underlyingRegistration())) { done =>
        done shouldBe a[Ack]
      }

      eventually {
        ref.registrationLinks().exists(l => l.componentName == "boomControl") shouldEqual true
      }

      ref
        .registrationLinks()
        .find(l => l.componentName == "boomControl")
        .foreach(link => {
          link.controlProxy.shutdown(probe.ref)
        })

      eventually {
        ref.registrationLinks().exists(l => l.componentName == "boomControl") shouldEqual false
      }
    }

    "receive signal" in testContext { ctx =>
      import ctx._
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

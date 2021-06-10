// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream.sliding

import java.util.concurrent.{ Executors, TimeUnit }

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito
import org.mockito.stubbing.Stubber
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import surge.health.SignalType
import surge.health.config.{ WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ Error, HealthSignal, HealthSignalBuilder, Trace }
import surge.health.matchers.SideEffect
import surge.health.windows.{ AddedToWindow, WindowAdvanced, WindowClosed, WindowOpened }
import surge.internal.health._
import surge.internal.health.matchers.{ RepeatingSignalMatcher, SignalNameEqualsMatcher }
import surge.internal.health.supervisor.HealthSignalReceived

import scala.concurrent.duration._

trait MockitoHelper extends MockitoSugar {
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}

class SlidingHealthSignalStreamSpec
    extends TestKit(ActorSystem("SlidingHealthSignalStreamSpec", ConfigFactory.load("sliding-health-signal-stream-spec")))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Eventually
    with MockitoHelper {
  implicit val postfixOps: languageFeature.postfixOps = scala.language.postfixOps
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(10, Milliseconds)))
  private var probe: TestProbe = _
  private var bus: HealthSignalBusInternal = _
  override def beforeEach(): Unit = {
    probe = TestProbe()
    val signal = HealthSignalBuilder("health.signal").withName(s"5 in a row").withSignalType(SignalType.TRACE).withData(Trace(s"5 in a row")).build()

    val filters = Seq(
      RepeatingSignalMatcher(
        times = 5,
        atomicMatcher =
          SignalNameEqualsMatcher("test.trace", Some(SideEffect(Seq(HealthSignal("health.signal", "boom", SignalType.ERROR, Error("bah", None)))))),
        Some(SideEffect(Seq(signal)))))
    val signalStreamProvider = new SlidingHealthSignalStreamProvider(
      WindowingStreamConfig(maxDelay = 1 second, advancerConfig = WindowingStreamSliderConfig()),
      system,
      streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
      filters)

    bus = signalStreamProvider.busWithSupervision(startStreamOnInit = true)
  }

  override def afterEach(): Unit = {
    Option(bus).foreach(b => b.unsupervise())
  }

  override def afterAll(): Unit = {
    system.terminate()
  }

  "SlidingHealthSignalStreamSpec" should {

    "subscribe to HealthSignalBus" in {
      eventually {
        bus.subscriberInfo().exists(info => info.name == "surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamImpl") shouldEqual true
      }
    }

    "not lose signals" in {
      bus.signalWithTrace("trace.test", Trace("tester")).emit().emit().emit()

      eventually {
        val closed = probe.fishForMessage(max = 100 millis) { case msg =>
          msg.isInstanceOf[WindowClosed]
        }

        closed.asInstanceOf[WindowClosed].d.signals.size shouldEqual 3

        val advanced = probe.fishForMessage(max = 100 millis) { case msg =>
          msg.isInstanceOf[WindowAdvanced]
        }
        advanced.asInstanceOf[WindowAdvanced].d.signals.size shouldEqual 3
      }
    }

    "receive multiple health signals aggregated in one WindowAdvance event" in {
      val scheduledEmit = Executors
        .newScheduledThreadPool(1)
        .scheduleAtFixedRate(
          () => bus.signalWithTrace(name = "test.trace", Trace("test.trace")).emit(),
          (5 seconds).toMillis,
          (5 seconds).toMillis,
          TimeUnit.MILLISECONDS)

      eventually {
        val msg = probe.fishForMessage(max = 1 second) { case msg =>
          msg.isInstanceOf[WindowAdvanced]
        }

        msg.isInstanceOf[WindowAdvanced] shouldEqual true
        msg.asInstanceOf[WindowAdvanced].d.signals.size == 2
      }

      scheduledEmit.cancel(true)
    }

    "open window when signal emitted" in {

      // Send error signal
      bus.signalWithError(name = "test.error", Error("error to test open-window", None)).emit()

      eventually {
        val windowOpened = probe.fishForMessage(max = 1 second) { case msg =>
          msg.isInstanceOf[WindowOpened]
        }

        windowOpened.asInstanceOf[WindowOpened].window().isDefined shouldEqual true
      }

    }

    "add to window when signal emitted" in {
      bus.signalWithError(name = "test.error", Error("error to test addtowindow", None)).emit()

      eventually {
        probe.expectMsgClass(classOf[AddedToWindow])
      }
    }

    "close an opened window" in {
      bus.signalWithError(name = "test.error", Error("error to test close-open-window", None)).emit()

      eventually {
        val windowClosed = probe.fishForMessage(max = 100 millis) { case msg =>
          msg.isInstanceOf[WindowClosed]
        }

        windowClosed.asInstanceOf[WindowClosed].window().isDefined shouldEqual true
        windowClosed.asInstanceOf[WindowClosed].d.signals.exists(s => s.name == "test.error") shouldEqual true
      }
    }

    "detect 5 messages in a 10 second window" in {
      bus.signalWithTrace(name = "test.trace", Trace("test.trace")).emit().emit().emit().emit().emit()

      eventually {
        val received: Any = probe.fishForMessage(max = 2 second) { case msg =>
          msg.isInstanceOf[HealthSignalReceived]
        }

        received shouldBe a[HealthSignalReceived]
        received.asInstanceOf[HealthSignalReceived].signal.name == "5 in a row" shouldEqual true
      }
    }
  }
}

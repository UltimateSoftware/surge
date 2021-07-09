// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream.sliding

import java.time.Instant
import java.util.concurrent.{ Executors, TimeUnit }

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito
import org.mockito.stubbing.Stubber
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }
import org.scalatestplus.mockito.MockitoSugar
import surge.health.config.{ ThrottleConfig, WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ Error, HealthSignal, Trace }
import surge.health.matchers.SideEffect
import surge.health.windows._
import surge.health.{ HealthSignalReceived, HealthSignalStream, SignalType }
import surge.internal.health._
import surge.internal.health.matchers.{ RepeatingSignalMatcher, SignalNameEqualsMatcher }
import surge.internal.health.windows.stream.WindowingHealthSignalStream

import scala.collection.mutable
import scala.concurrent.duration._

trait MockitoHelper extends MockitoSugar {
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}

case class TimedCount(count: Int, time: Option[Instant])
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
  private var signalStreamProvider: SlidingHealthSignalStreamProvider = _

  override def beforeEach(): Unit = {
    probe = TestProbe()
    val signal = HealthSignal(topic = "health.signal", name = s"5 in a row", signalType = SignalType.TRACE, data = Trace(s"5 in a row"))

    val filters = Seq(
      RepeatingSignalMatcher(
        times = 5,
        atomicMatcher =
          SignalNameEqualsMatcher("test.trace", Some(SideEffect(Seq(HealthSignal("health.signal", "boom", SignalType.ERROR, Error("bah", None)))))),
        Some(SideEffect(Seq(signal)))))
    signalStreamProvider = new SlidingHealthSignalStreamProvider(
      WindowingStreamConfig(
        advancerConfig = WindowingStreamSliderConfig(buffer = 10, advanceAmount = 1),
        throttleConfig = ThrottleConfig(100, 5.seconds),
        windowingDelay = 1.seconds,
        maxWindowSize = 500,
        frequencies = Seq(10.seconds)),
      system,
      streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
      filters)

    bus = signalStreamProvider.busWithSupervision(startStreamOnInit = true)
  }

  override def afterEach(): Unit = {
    Option(bus).foreach(b => b.unsupervise())
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "SlidingHealthSignalStream" should {
    "subscribe to HealthSignalBus" in {
      eventually {
        bus.subscriberInfo().exists(info => info.name == "surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamImpl") shouldEqual true
      }
    }

    "not lose signals" in {
      bus.signalWithTrace(name = "trace.test", Trace("tester")).emit().emit().emit()

      eventually {
        val closed = probe.fishForMessage(max = 400.millis) { case msg =>
          msg.isInstanceOf[WindowClosed]
        }

        closed.asInstanceOf[WindowClosed].d.signals.size shouldEqual 3

        val advanced = probe.fishForMessage(max = 200.millis) { case msg =>
          msg.isInstanceOf[WindowAdvanced]
        }
        advanced.asInstanceOf[WindowAdvanced].d.signals.size shouldEqual 3
      }
    }

    "provide source of WindowEvents" in {
      val signalStream: Option[HealthSignalStream] = bus.backingSignalStream()

      signalStream.isDefined shouldEqual true
      signalStream.get shouldBe a[WindowingHealthSignalStream]

      val windowEventSource: Source[WindowEvent, NotUsed] =
        signalStream.get.asInstanceOf[WindowingHealthSignalStream].windowEventSource().source

      val receivedWindowEvents: mutable.ArrayBuffer[WindowEvent] = new mutable.ArrayBuffer[WindowEvent]()

      windowEventSource.runWith(Sink.foreach(event => {
        receivedWindowEvents.addOne(event)
      }))

      bus.signalWithError(name = "test.error", Error("error to test close-open-window", None)).emit()

      eventually {
        receivedWindowEvents.nonEmpty shouldEqual true

        receivedWindowEvents.exists(e => {
          e.isInstanceOf[AddedToWindow] && e.asInstanceOf[AddedToWindow].s.name == "test.error"
        }) shouldEqual true
      }
    }

    "receive multiple health signals aggregated in one WindowAdvance event" in {
      val scheduledEmit = Executors
        .newScheduledThreadPool(1)
        .scheduleAtFixedRate(
          () => bus.signalWithTrace(name = "test.trace", Trace("test.trace")).emit(),
          5.seconds.toMillis,
          5.seconds.toMillis,
          TimeUnit.MILLISECONDS)

      eventually {
        val msg = probe.fishForMessage(max = 2.second) { case msg =>
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
        val windowOpened = probe.fishForMessage(max = 1.second) { case msg =>
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
        val windowClosed = probe.fishForMessage(max = 400.millis) { case msg =>
          msg.isInstanceOf[WindowClosed]
        }

        windowClosed.asInstanceOf[WindowClosed].window().isDefined shouldEqual true
        windowClosed.asInstanceOf[WindowClosed].d.signals.exists(s => s.name == "test.error") shouldEqual true
      }
    }

    "detect 5 messages in a 10 second window" in {
      bus.signalWithTrace(name = "test.trace", Trace("test.trace")).emit().emit().emit().emit().emit()

      eventually {
        val received: Any = probe.fishForMessage(max = 2.seconds) { case msg =>
          msg.isInstanceOf[HealthSignalReceived]
        }

        received shouldBe a[HealthSignalReceived]
        received.asInstanceOf[HealthSignalReceived].signal.name == "5 in a row" shouldEqual true
      }
    }
  }
}

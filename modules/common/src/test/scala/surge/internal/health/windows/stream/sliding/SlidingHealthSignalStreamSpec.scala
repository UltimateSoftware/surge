// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream.sliding

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
import surge.health.matchers.{ SideEffect, SignalPatternMatcherDefinition }
import surge.health.windows._
import surge.health.{ HealthSignalStream, SignalType }
import surge.internal.health._
import surge.internal.health.windows.stream.WindowingHealthSignalStream

import java.time.Instant
import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer
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
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(10, Milliseconds)))
  private var probe: TestProbe = _
  private var bus: HealthSignalBusInternal = _
  private var signalStreamProvider: SlidingHealthSignalStreamProvider = _

  override def beforeEach(): Unit = {
    probe = TestProbe()
    val signal = HealthSignal(topic = "health.signal", name = s"5 in a row", signalType = SignalType.TRACE, data = Trace(s"5 in a row"), source = None)

    val definition = SignalPatternMatcherDefinition.repeating(
      times = 5,
      pattern = Pattern.compile("test.trace"),
      frequency = 10.seconds,
      sideEffect = Some(SideEffect(Seq(signal))))

    signalStreamProvider = new SlidingHealthSignalStreamProvider(
      WindowingStreamConfig(
        advancerConfig = WindowingStreamSliderConfig(buffer = 5, advanceAmount = 1),
        throttleConfig = ThrottleConfig(100, 10.seconds),
        windowingInitDelay = 100.millis,
        windowingResumeDelay = 1.seconds,
        maxWindowSize = 500),
      system,
      streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
      patternMatchers = Seq(definition))

    bus = signalStreamProvider.bus()
  }

  override def afterEach(): Unit = {
    Option(bus).foreach(b => b.unsupervise())
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  "SlidingHealthSignalStream" should {
    "subscribe to HealthSignalBus" in {
      eventually {
        bus.subscriberInfo().exists(info => info.name == "surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamImpl") shouldEqual true
      }
    }

    "not fail on repeated starts" in {
      bus.signalStream().start().start().start()
    }

    "not lose signals" in {
      val windowEventSourceWithQueue = bus.signalStream().asInstanceOf[WindowingHealthSignalStream].windowEventSource()

      val windowEventBucket: ArrayBuffer[WindowEvent] = ArrayBuffer[WindowEvent]()
      windowEventSourceWithQueue.source.runWith(Sink.foreach(event => windowEventBucket.addOne(event)))

      bus.signalWithTrace(name = "trace.test", Trace("tester")).emit().emit().emit().emit().emit()

      eventually {
        // 1 advance event
        val advancedEvents = windowEventBucket.filter(e => e.isInstanceOf[WindowAdvanced])
        advancedEvents.size shouldEqual 1

        // 5 signals captured in advance event
        advancedEvents.head.asInstanceOf[WindowAdvanced].d.signals.size shouldEqual 5

      }
    }

    "provide source of WindowEvents" in {
      val signalStream: Option[HealthSignalStream] = bus.backingSignalStream()

      signalStream.isDefined shouldEqual true
      signalStream.get shouldBe a[WindowingHealthSignalStream]

      val windowEventSource: Source[WindowEvent, NotUsed] =
        signalStream.get.asInstanceOf[WindowingHealthSignalStream].windowEventSource().source

      val receivedWindowEvents: ArrayBuffer[WindowEvent] = new ArrayBuffer[WindowEvent]()

      windowEventSource.runWith(Sink.foreach(event => receivedWindowEvents.addOne(event)))

      bus.signalWithError(name = "test.error", Error("error to test close-open-window", None)).emit()

      eventually {
        receivedWindowEvents.nonEmpty shouldEqual true

        receivedWindowEvents.exists(e => {
          e.isInstanceOf[AddedToWindow] && e.asInstanceOf[AddedToWindow].s.name == "test.error"
        }) shouldEqual true
      }
    }

    "add to window when signal emitted" in {
      val signalStream: Option[HealthSignalStream] = bus.backingSignalStream()

      val windowEventSource: Source[WindowEvent, NotUsed] =
        signalStream.get.asInstanceOf[WindowingHealthSignalStream].windowEventSource().source

      val receivedWindowEvents: ArrayBuffer[WindowEvent] = new ArrayBuffer[WindowEvent]()

      windowEventSource.runWith(Sink.foreach(event => receivedWindowEvents.addOne(event)))

      bus.signalWithError(name = "test.error", Error("error to test addToWindow", None)).emit()

      eventually {
        receivedWindowEvents.count(event => event.isInstanceOf[AddedToWindow]) shouldEqual 1
      }
    }

    "detect 5 messages" in {
      val signalStream: HealthSignalStream = bus.signalStream()
      val signalSourceWithQueue = signalStream.signalSource(10, ThrottleConfig(10, 10.seconds))
      val signalBucket: ArrayBuffer[HealthSignal] = ArrayBuffer[HealthSignal]()
      signalSourceWithQueue.source.runWith(Sink.foreach(signal => signalBucket.addOne(signal)))

      bus.signalWithTrace(name = "test.trace", Trace("test.trace")).emit().emit().emit().emit().emit()

      eventually {
        signalBucket.size shouldEqual 5
      }
    }
  }
}

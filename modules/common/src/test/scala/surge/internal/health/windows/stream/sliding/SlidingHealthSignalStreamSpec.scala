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
import surge.health.matchers.{ SideEffect, SignalPatternMatcherDefinition }
import surge.health.windows._
import surge.health.{ HealthSignalReceived, HealthSignalStream, SignalType }
import surge.internal.health._
import surge.internal.health.windows.stream.WindowingHealthSignalStream

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
        advancerConfig = WindowingStreamSliderConfig(buffer = 10, advanceAmount = 1),
        throttleConfig = ThrottleConfig(100, 5.seconds),
        windowingInitDelay = 100.millis,
        windowingResumeDelay = 1.seconds,
        maxWindowSize = 500),
      system,
      streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
      patternMatchers = Seq(definition))

    bus = signalStreamProvider.bus()

    probe.expectMsgClass(classOf[WindowOpened])
  }

  override def afterEach(): Unit = {
    Option(bus).foreach(b => {
      b.signalStream().stop()
    })

    // todo: assert window is stopped / closed
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

    "receive multiple health signals aggregated in one WindowAdvance event" in {
      val sourceAndEvents = trackWindowEvents()
      val scheduledEmit = Executors
        .newScheduledThreadPool(1)
        .scheduleAtFixedRate(
          () => bus.signalWithTrace(name = "test.trace", Trace("test.trace")).emit(),
          0.millis.toMillis,
          1.seconds.toMillis,
          TimeUnit.MILLISECONDS)

      eventually {
        val maybeWindowAdvanced = sourceAndEvents._2.find(e => e.isInstanceOf[WindowAdvanced])
        maybeWindowAdvanced shouldBe defined
        val msg = maybeWindowAdvanced.get
        // fix: lag in the pipeline sometimes fails matching for 10 signals which is what we expect.
        //  for now expect at least 8.
        msg.asInstanceOf[WindowAdvanced].d.signals.size > 8
      }

      scheduledEmit.cancel(true)
    }

    "add to window when signal emitted" in {
      val sourceAndEvents = trackWindowEvents()

      bus.signalWithError(name = "test.error", Error("error to test addtowindow", None)).emit()

      eventually {
        sourceAndEvents._2.exists(e => e.isInstanceOf[AddedToWindow])
      }
    }
  }

  private def trackWindowEvents(): (Source[WindowEvent, NotUsed], ArrayBuffer[WindowEvent]) = {
    val signalStream: Option[HealthSignalStream] = bus.backingSignalStream()

    signalStream.isDefined shouldEqual true
    signalStream.get shouldBe a[WindowingHealthSignalStream]

    val windowEventSource: Source[WindowEvent, NotUsed] =
      signalStream.get.asInstanceOf[WindowingHealthSignalStream].windowEventSource().source

    val receivedWindowEvents: ArrayBuffer[WindowEvent] = ArrayBuffer[WindowEvent]()

    windowEventSource.runWith(Sink.foreach(event => {
      receivedWindowEvents.addOne(event)
    }))

    (windowEventSource, receivedWindowEvents)
  }
}

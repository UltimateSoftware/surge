// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.matchers

import java.util.regex.Pattern
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.testkit.{ TestKit, TestProbe }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.health.SignalType
import surge.health.config.{ ThrottleConfig, WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ HealthSignal, HealthSignalSource, Trace }
import surge.health.matchers.{ SideEffect, SignalPatternMatcherDefinition }
import surge.health.windows.WindowAdvanced
import surge.internal.health.StreamMonitoringRef
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.languageFeature.postfixOps

class RepeatingSignalMatcherSpec extends TestKit(ActorSystem("RepeatingSignals")) with AnyWordSpecLike with BeforeAndAfterAll with Matchers {
  implicit val postOp: postfixOps = postfixOps

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  private val testTraceSignal = HealthSignal(topic = "topic", name = "test.trace", signalType = SignalType.TRACE, data = Trace("test"), source = None)
  private val testSignal1 = HealthSignal(topic = "topic", name = "foo", signalType = SignalType.TRACE, data = Trace("test"), source = None)
  private val testSignal2 = HealthSignal(topic = "topic", name = "bar", signalType = SignalType.TRACE, data = Trace("test"), source = None)
  private val fiveInARowSignal = HealthSignal(topic = "topic", name = "5 in a row", signalType = SignalType.TRACE, data = Trace("test"), source = None)

  val signalTopic: String = "topic"
  "RepeatingSignalPatternMatcher" should {
    "work with a Sliding Stream" in {
      val windowBuffer = 10
      val probe = TestProbe()
      val slidingHealthSignalStream = new SlidingHealthSignalStreamProvider(
        WindowingStreamConfig(
          advancerConfig = WindowingStreamSliderConfig(buffer = windowBuffer, advanceAmount = 1),
          throttleConfig = ThrottleConfig(elements = 100, duration = 5.seconds),
          windowingInitDelay = 5.seconds,
          windowingResumeDelay = 5.seconds,
          maxWindowSize = 500),
        patternMatchers = Seq(SignalPatternMatcherDefinition.repeating(times = 2, pattern = Pattern.compile("99"), frequency = 10.seconds, sideEffect = None)),
        streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
        actorSystem = system)

      val bus = slidingHealthSignalStream.bus()

      val repeatingData = Seq(
        Range(1, 100),
        Range(1, 100),
        Range(1, 100),
        Range(1, 100),
        Range(1, 100),
        Range(1, 100),
        Range(1, 100),
        Range(1, 100),
        Range(1, 100),
        Range(1, 100))

      Source(repeatingData)
        .mapAsync(parallelism = 10)(data =>
          Future {
            data.foreach(d => {
              bus.signalWithTrace(name = s"$d", trace = Trace("trace", None, None)).emit()
            })
          }(system.dispatcher))
        .run()

      val advanced = probe.fishForMessage() { case msg =>
        msg.isInstanceOf[WindowAdvanced]
      }

      advanced shouldBe a[WindowAdvanced]
      bus.unsupervise()
    }

    "Find 5 matching signals - using nameEquals" in {
      val matcher =
        RepeatingSignalMatcher(times = 5, atomicMatcher = SignalNameEqualsMatcher(name = "test.trace"), sideEffect = Some(SideEffect(Seq(fiveInARowSignal))))

      val result = matcher.searchForMatch(
        sourceFromData(Seq(testTraceSignal, testTraceSignal, testSignal1, testTraceSignal, testSignal2, testTraceSignal, testTraceSignal, testSignal2)),
        frequency = 10.seconds)

      result.matches.size shouldEqual 5
    }

    "Find 5 matching signals - using pattern" in {
      val matcher =
        RepeatingSignalMatcher(
          times = 5,
          atomicMatcher = SignalNamePatternMatcher(pattern = Pattern.compile("test.trace")),
          sideEffect = Some(SideEffect(Seq(fiveInARowSignal))))

      val result = matcher.searchForMatch(
        sourceFromData(Seq(testTraceSignal, testTraceSignal, testSignal1, testTraceSignal, testSignal2, testTraceSignal, testTraceSignal)),
        frequency = 10.seconds)

      result.matches.size shouldEqual 5
    }

    "Find 0 matching signals - using nameEquals" in {
      val matcher =
        RepeatingSignalMatcher(times = 5, atomicMatcher = SignalNameEqualsMatcher(name = "test_trace"), sideEffect = Some(SideEffect(Seq(fiveInARowSignal))))

      val result =
        matcher.searchForMatch(sourceFromData(Seq(testSignal1, testTraceSignal, testSignal2)), frequency = 10.seconds)

      result.matches.size shouldEqual 0
    }

    "Find 0 matching signals - using pattern" in {
      val matcher =
        RepeatingSignalMatcher(
          times = 5,
          atomicMatcher = SignalNamePatternMatcher(pattern = Pattern.compile("test_trace")),
          sideEffect = Some(SideEffect(Seq(fiveInARowSignal))))

      val result =
        matcher.searchForMatch(sourceFromData(Seq(testSignal1, testTraceSignal, testSignal2)), frequency = 10.seconds)

      result.matches.size shouldEqual 0
    }
  }

  private def sourceFromData(data: Seq[HealthSignal]): HealthSignalSource =
    new HealthSignalSource {
      override def flush(): Unit = {}

      override def signals(): Seq[HealthSignal] = data
    }
}

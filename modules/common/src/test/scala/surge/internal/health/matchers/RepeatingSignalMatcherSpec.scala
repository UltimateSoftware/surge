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
import surge.health.config.{ WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ HealthSignalBuilder, Trace }
import surge.health.matchers.SideEffect
import surge.health.windows.WindowAdvanced
import surge.internal.health.StreamMonitoringRef
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.languageFeature.postfixOps

class RepeatingSignalMatcherSpec extends TestKit(ActorSystem("RepeatingSignals")) with AnyWordSpecLike with BeforeAndAfterAll with Matchers {
  implicit val postOp: postfixOps = postfixOps

  override def afterAll(): Unit =
    system.terminate()

  val signalTopic: String = "topic"
  "RepeatingSignalPatternMatcher" should {

    "work with a Sliding Stream" in {
      val probe = TestProbe()
      val slidingHealthSignalStream = new SlidingHealthSignalStreamProvider(
        WindowingStreamConfig(advancerConfig = WindowingStreamSliderConfig()),
        filters = Seq(RepeatingSignalMatcher(2, SignalNameEqualsMatcher("99"), None)),
        streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
        actorSystem = system)

      val bus = slidingHealthSignalStream.busWithSupervision(startStreamOnInit = true)

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

      // expect only 11 elements in data because that is the configured buffer + 1
      advanced.asInstanceOf[WindowAdvanced].d.signals.size shouldEqual 10 +- 1
      bus.unsupervise()
    }

    "Find 5 matching signals - using nameEquals" in {
      val signal = HealthSignalBuilder("topic").withName(name = s"5 in a row").withSignalType(SignalType.TRACE).withData(Trace(s"5 in a row")).build()

      val matcher = RepeatingSignalMatcher(times = 5, atomicMatcher = SignalNameEqualsMatcher(name = "test.trace"), Some(SideEffect(Seq(signal))))

      val result = matcher.searchForMatch(
        Seq(
          HealthSignalBuilder("topic").withName(name = "test.trace").build(),
          HealthSignalBuilder("topic").withName(name = "test.trace").build(),
          HealthSignalBuilder("topic").withName(name = "foo").build(),
          HealthSignalBuilder("topic").withName(name = "test.trace").build(),
          HealthSignalBuilder("topic").withName(name = "bar").build(),
          HealthSignalBuilder("topic").withName(name = "test.trace").build(),
          HealthSignalBuilder("topic").withName(name = "test.trace").build()),
        frequency = 10.seconds)

      result.matches.size shouldEqual 5
    }

    "Find 5 matching signals - using pattern" in {
      val signal = HealthSignalBuilder("topic").withName(name = s"5 in a row").withSignalType(SignalType.TRACE).withData(Trace(s"5 in a row")).build()

      val matcher =
        RepeatingSignalMatcher(times = 5, atomicMatcher = SignalNamePatternMatcher(pattern = Pattern.compile("test.trace")), Some(SideEffect(Seq(signal))))

      val result = matcher.searchForMatch(
        Seq(
          HealthSignalBuilder("topic").withName(name = "test.trace").build(),
          HealthSignalBuilder("topic").withName(name = "test.trace").build(),
          HealthSignalBuilder("topic").withName(name = "foo").build(),
          HealthSignalBuilder("topic").withName(name = "test.trace").build(),
          HealthSignalBuilder("topic").withName(name = "bar").build(),
          HealthSignalBuilder("topic").withName(name = "test.trace").build(),
          HealthSignalBuilder("topic").withName(name = "test.trace").build()),
        frequency = 10.seconds)

      result.matches.size shouldEqual 5
    }

    "Find 0 matching signals - using nameEquals" in {
      val signal = HealthSignalBuilder("topic").withName(s"5 in a row").withSignalType(SignalType.TRACE).withData(Trace(s"5 in a row")).build()

      val matcher = RepeatingSignalMatcher(times = 5, atomicMatcher = SignalNameEqualsMatcher(name = "test_trace"), Some(SideEffect(Seq(signal))))

      val result = matcher.searchForMatch(
        Seq(
          HealthSignalBuilder("topic").withName(name = "foo").build(),
          HealthSignalBuilder("topic").withName(name = "test.trace").build(),
          HealthSignalBuilder("topic").withName(name = "bar").build()),
        frequency = 10.seconds)

      result.matches.size shouldEqual 0
    }

    "Find 0 matching signals - using pattern" in {
      val signal = HealthSignalBuilder("topic").withName(s"5 in a row").withSignalType(SignalType.TRACE).withData(Trace(s"5 in a row")).build()

      val matcher =
        RepeatingSignalMatcher(times = 5, atomicMatcher = SignalNamePatternMatcher(pattern = Pattern.compile("test_trace")), Some(SideEffect(Seq(signal))))

      val result = matcher.searchForMatch(
        Seq(
          HealthSignalBuilder("topic").withName(name = "foo").build(),
          HealthSignalBuilder("topic").withName(name = "test.trace").build(),
          HealthSignalBuilder("topic").withName(name = "bar").build()),
        frequency = 10.seconds)

      result.matches.size shouldEqual 0
    }
  }
}

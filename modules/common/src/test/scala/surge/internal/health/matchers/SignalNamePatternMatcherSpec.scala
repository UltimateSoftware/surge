// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.matchers

import org.mockito.Mockito.{ mock, when }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import surge.health.domain.{ HealthSignal, HealthSignalSource }

import scala.languageFeature.postfixOps
import scala.concurrent.duration._

class SignalNamePatternMatcherSpec extends AnyWordSpec with Matchers {
  implicit val postOp: postfixOps = postfixOps

  "SignalNameRegexMatcher" should {
    "find signals matching name beginning with chars" in {
      val matcher = SignalNamePatternMatcher.beginsWith("fr")
      val signalNamedFred = mock(classOf[HealthSignal])
      when(signalNamedFred.name).thenReturn("fred")
      val result = matcher.searchForMatch(sourceFromData(Seq(signalNamedFred)), frequency = 10.seconds)

      result.matches.map(s => s.name) shouldEqual Seq("fred")
      result.found() shouldEqual true
      result.hasSideEffects shouldEqual false
    }

    "find signals matching name ending with chars" in {
      val matcher = SignalNamePatternMatcher.endsWith("ed")
      val signalNamedFred = mock(classOf[HealthSignal])
      when(signalNamedFred.name).thenReturn("fred")
      val result = matcher.searchForMatch(sourceFromData(Seq(signalNamedFred)), frequency = 10.seconds)

      result.matches.map(s => s.name) shouldEqual Seq("fred")
      result.found() shouldEqual true
      result.hasSideEffects shouldEqual false
    }
  }

  private def sourceFromData(data: Seq[HealthSignal]): HealthSignalSource = new HealthSignalSource() {
    override def signals(): Seq[HealthSignal] = data

    override def flush(): Unit = {}
  }
}

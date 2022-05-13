// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.matchers

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.mockito.Mockito._
import surge.health.domain.{ HealthSignal, HealthSignalSource }
import surge.health.matchers.SideEffect

import scala.concurrent.duration._
import scala.languageFeature.postfixOps

class SignalNameEqualsMatcherSpec extends AnyWordSpec with Matchers {

  "SignalNameMatcher" should {
    "find signals matching specified name" in {
      val matcher = SignalNameEqualsMatcher(name = "fred")
      val signalNamedFred = mock(classOf[HealthSignal])
      when(signalNamedFred.name).thenReturn("fred")
      val result = matcher.searchForMatch(sourceFromData(Seq(signalNamedFred)), frequency = 10.seconds)

      result.matches.map(s => s.name) shouldEqual Seq("fred")
      result.found() shouldEqual true
      result.hasSideEffects shouldEqual false
    }

    "given match found; return result with SideEffect" in {
      val matcher = SignalNameEqualsMatcher(name = "bob", sideEffect = Some(SideEffect(Seq(mock(classOf[HealthSignal])))))
      val signalNamedBob = mock(classOf[HealthSignal])
      when(signalNamedBob.name).thenReturn("bob")

      val result = matcher.searchForMatch(sourceFromData(Seq(signalNamedBob)), frequency = 10.seconds)
      result.hasSideEffects shouldEqual true
      result.found() shouldEqual true
    }

    "given no SideEffect and match found; return result without SideEffect" in {
      val matcher = SignalNameEqualsMatcher(name = "bob", sideEffect = None)
      val signalNamedBob = mock(classOf[HealthSignal])
      when(signalNamedBob.name).thenReturn("bob")

      val result = matcher.searchForMatch(sourceFromData(Seq(signalNamedBob)), frequency = 10.seconds)
      result.hasSideEffects shouldEqual false
      result.found() shouldEqual true
    }

    "given empty SideEffect and match found; return result without SideEffect" in {
      val matcher = SignalNameEqualsMatcher(name = "bob", sideEffect = Some(SideEffect(Seq())))
      val signalNamedBob = mock(classOf[HealthSignal])
      when(signalNamedBob.name).thenReturn("bob")

      val result = matcher.searchForMatch(sourceFromData(Seq(signalNamedBob)), frequency = 10.seconds)
      result.hasSideEffects shouldEqual false
      result.found() shouldEqual true
    }

    "given match not found; return result without SideEffect" in {
      val matcher = SignalNameEqualsMatcher(name = "bob", sideEffect = Some(SideEffect(Seq(mock(classOf[HealthSignal])))))
      val signalNamedJorge = mock(classOf[HealthSignal])
      when(signalNamedJorge.name).thenReturn("jorge")

      val result = matcher.searchForMatch(sourceFromData(Seq(signalNamedJorge)), frequency = 10.seconds)
      result.hasSideEffects shouldEqual false
      result.found() shouldEqual false
    }

    "given no SideEffect and match not found; return result without SideEffect" in {
      val matcher = SignalNameEqualsMatcher(name = "bob", sideEffect = None)
      val signalNamedJorge = mock(classOf[HealthSignal])
      when(signalNamedJorge.name).thenReturn("jorge")

      val result = matcher.searchForMatch(sourceFromData(Seq(signalNamedJorge)), frequency = 10.seconds)
      result.hasSideEffects shouldEqual false
      result.found() shouldEqual false
    }

    "given empty SideEffect and match not found; return result without SideEffect" in {
      val matcher = SignalNameEqualsMatcher(name = "bob", sideEffect = Some(SideEffect(Seq())))
      val signalNamedJorge = mock(classOf[HealthSignal])
      when(signalNamedJorge.name).thenReturn("jorge")

      val result = matcher.searchForMatch(sourceFromData(Seq(signalNamedJorge)), frequency = 10.seconds)
      result.hasSideEffects shouldEqual false
      result.found() shouldEqual false
    }
  }

  private def sourceFromData(data: Seq[HealthSignal]): HealthSignalSource = new HealthSignalSource {
    override def signals(): Seq[HealthSignal] = data

    override def flush(): Unit = {}
  }
}

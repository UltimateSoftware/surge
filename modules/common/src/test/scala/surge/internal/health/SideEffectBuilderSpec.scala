// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.mockito.Mockito._
import surge.health.domain.HealthSignal
import surge.health.matchers.SideEffectBuilder

class SideEffectBuilderSpec extends AnyWordSpec with Matchers {

  "SideEffectBuilder" should {
    "build SideEffect" in {
      val builder = SideEffectBuilder()
      val sideEffect = builder.addSideEffectSignal(mock(classOf[HealthSignal])).buildSideEffect()

      sideEffect.signals.size shouldEqual 1
    }

    "clear signals on reset" in {
      val builder = SideEffectBuilder()
      val sideEffectBuilder = builder.addSideEffectSignal(mock(classOf[HealthSignal]))

      val sideEffect = sideEffectBuilder.buildSideEffect()

      sideEffect.signals.size shouldEqual 1

      val sideEffectAfterReset = sideEffectBuilder.reset().buildSideEffect()
      sideEffectAfterReset.signals.size shouldEqual 0
      (sideEffect.signals.size should not).equal(sideEffectAfterReset.signals.size)
    }
  }
}

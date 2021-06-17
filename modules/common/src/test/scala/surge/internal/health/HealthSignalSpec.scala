// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import org.mockito.{ ArgumentMatchers, Mockito }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.mockito.Mockito._
import surge.health.domain.HealthSignal
class HealthSignalSpec extends AnyWordSpec with Matchers {
  "EmittableHealthSignal" should {
    "invoke publish on HealthSignalBus when emitting" in {
      val bus = mock(classOf[HealthSignalBusInternal])

      val signal = new EmittableHealthSignalImpl(mock(classOf[HealthSignal]), signalBus = bus)

      signal.emit()

      Mockito.verify(bus, times(1)).publish(ArgumentMatchers.any(classOf[HealthSignal]))
    }
  }
}

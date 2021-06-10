// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.windows

import surge.health.domain.HealthSignal

import scala.concurrent.duration.FiniteDuration

case class WindowData(signals: Seq[HealthSignal], frequency: FiniteDuration)

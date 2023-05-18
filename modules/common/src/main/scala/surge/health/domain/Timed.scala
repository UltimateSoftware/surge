// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health.domain

import java.time.Instant

trait Timed {
  def timestamp: Instant
}

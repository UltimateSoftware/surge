// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

trait AutoUpdatingMetrics {
  def update(): Unit
}

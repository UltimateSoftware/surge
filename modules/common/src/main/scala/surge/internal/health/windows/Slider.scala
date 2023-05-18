// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows

import surge.health.windows.Advancer

trait Slider[IT] extends Advancer[IT] {

  def slide(it: IT, amount: Int, force: Boolean = false): Option[IT]

  def slideAmount(): Int

  override def advance(it: IT, force: Boolean = false): Option[IT] = {
    slide(it, slideAmount(), force)
  }

  override def buffer(): Int = 10
}

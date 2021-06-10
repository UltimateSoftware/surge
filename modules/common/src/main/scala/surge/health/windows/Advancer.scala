// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.windows

trait Advancer[IT] {
  def advance(it: IT, force: Boolean = false): Option[IT]
  def buffer(): Int
}

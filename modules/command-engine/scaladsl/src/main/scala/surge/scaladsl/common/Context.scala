// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.common

import surge.internal
import surge.internal.persistence

trait Context {}

object Context {
  def apply(core: persistence.Context): Context = new ContextImpl(core)
}

class ContextImpl(private val core: persistence.Context) extends Context
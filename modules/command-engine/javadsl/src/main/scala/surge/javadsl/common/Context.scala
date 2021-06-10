// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.common

import surge.core.{ Context => Core }

trait Context {}

object Context {
  def apply(core: Core): Context = new ContextImpl(core)
}

class ContextImpl(private val core: Core) extends Context

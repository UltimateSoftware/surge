// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal

import scala.util.Try

package object domain {
  /**
   * Defines a function type that tries applies a command to an aggregate
   */
  type CommandProcessor[Agg, Cmd, Evt] = (Option[Agg], Cmd) => Try[Seq[Evt]]

  /**
   * Defines a function type that applies an event to an aggregate
   */
  type EventHandler[Agg, Evt] = (Option[Agg], Evt) => Option[Agg]
}

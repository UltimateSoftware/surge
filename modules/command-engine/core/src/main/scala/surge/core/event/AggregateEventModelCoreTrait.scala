// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.event

import surge.internal.domain.EventHandler

trait AggregateEventModelCoreTrait[S, E] {
  def toCore: EventHandler[S, E]
}

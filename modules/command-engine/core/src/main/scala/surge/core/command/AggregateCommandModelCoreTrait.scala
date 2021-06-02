// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.command

import surge.internal.domain.CommandHandler

trait AggregateCommandModelCoreTrait[S, M, R, E] {
  def toCore: CommandHandler[S, M, R, E]
}

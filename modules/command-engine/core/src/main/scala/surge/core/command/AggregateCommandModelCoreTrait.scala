// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.command

import surge.internal.domain.CommandHandler

trait AggregateCommandModelCoreTrait[State, Message, Rejection, Event, Response] {
  def toCore: CommandHandler[State, Message, Rejection, Event, Response]
}

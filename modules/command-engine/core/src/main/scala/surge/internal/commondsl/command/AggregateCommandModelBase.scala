//
package surge.internal.commondsl.command

import surge.internal.domain.CommandHandler

trait AggregateCommandModelBase[S, M, R, E] {
  def toCore: CommandHandler[S, M, R, E]
}

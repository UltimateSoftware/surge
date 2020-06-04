// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.oss.domain

trait CommandProcessingException extends Exception
final case class CommandFailedException(msg: String) extends CommandProcessingException
final case class CommandUnauthorizedException(msg: String) extends CommandProcessingException

/**
 * AggregateCommandModel define a structure for a domain's types and algebra
 *
 * Implementations of this trait can be injected into
 *   UltiLagomPersistentEntity
 *   and soon, Surge and perhaps other persistence models (CRUD, Command Sourcing, etc)
 *
 * @tparam Agg  The Aggregate Type
 * @tparam Cmd  The Aggregate's Base Command Type
 * @tparam Evt  The Aggregate's Base Event Type
 * @tparam CmdMeta The Meta Data Type for Commands
 * @tparam EvtMeta The Meta Data Type for Events
 */
trait AggregateCommandModel[AggId, Agg, Cmd, Evt, CmdMeta, EvtMeta] {

  def aggIdFromCommand: Cmd ⇒ AggId

  def cmdMetaToEvtMeta: CmdMeta ⇒ EvtMeta

  def processCommand: CommandProcessor[Agg, Cmd, Evt, CmdMeta]
  def handleEvent: EventHandler[Agg, Evt, EvtMeta]
}

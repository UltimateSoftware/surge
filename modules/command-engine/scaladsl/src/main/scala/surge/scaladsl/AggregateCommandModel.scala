// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scaladsl

import surge.internal.domain.{ CommandProcessor, EventHandler }

import scala.util.Try

trait AggregateCommandModel[Agg, Cmd, Evt] {
  def processCommand(aggregate: Option[Agg], command: Cmd): Try[Seq[Evt]]
  def handleEvent(aggregate: Option[Agg], event: Evt): Option[Agg]

  private def doProcessCommand(aggregate: Option[Agg], command: Cmd): Try[Seq[Evt]] = processCommand(aggregate, command)
  private def doHandleEvent(aggregate: Option[Agg], event: Evt): Option[Agg] = handleEvent(aggregate, event)

  private[scaladsl] def toCore: surge.internal.domain.AggregateCommandModel[Agg, Cmd, Evt] = new surge.internal.domain.AggregateCommandModel[Agg, Cmd, Evt] {
    override def processCommand: CommandProcessor[Agg, Cmd, Evt] = { (agg, cmd) =>
      doProcessCommand(agg, cmd)
    }

    override def handleEvent: EventHandler[Agg, Evt] = { (agg, evt) =>
      doHandleEvent(agg, evt)
    }
  }
}

// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.javadsl

import java.util.Optional

import surge.internal.domain.{ CommandProcessor, EventHandler }

import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._
import scala.util.Try

abstract class AggregateCommandModel[Agg, Cmd, Evt] {
  def processCommand(aggregate: Optional[Agg], command: Cmd): java.util.List[Evt]
  def handleEvent(aggregate: Optional[Agg], event: Evt): Optional[Agg]

  private def doProcessCommand(aggregate: Optional[Agg], command: Cmd): java.util.List[Evt] = processCommand(aggregate, command)
  private def doHandleEvent(aggregate: Optional[Agg], event: Evt): Optional[Agg] = handleEvent(aggregate, event)
  private[javadsl] def toCore: surge.internal.domain.AggregateCommandModel[Agg, Cmd, Evt] = new surge.internal.domain.AggregateCommandModel[Agg, Cmd, Evt] {
    override def processCommand: CommandProcessor[Agg, Cmd, Evt] = { (agg, cmd) =>
      Try {
        doProcessCommand(agg.asJava, cmd).asScala.toVector
      }
    }

    override def handleEvent: EventHandler[Agg, Evt] = { (agg, evt) =>
      doHandleEvent(agg.asJava, evt).asScala
    }
  }
}

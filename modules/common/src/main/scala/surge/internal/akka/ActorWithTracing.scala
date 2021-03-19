// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.akka

import akka.AroundReceiveActor
import io.opentracing.Scope
import surge.internal.utils.SpanSupport
import surge.tracing.TracedMessage

trait ActorWithTracing extends AroundReceiveActor with SpanSupport {
  private var activeScope: Option[Scope] = None

  def tracedMessage[T](msg: T): TracedMessage[T] = {
    TracedMessage(tracer, msg, tracer.activeSpan())
  }

  override def doAroundReceive(receive: Receive, msg: Any): Unit = {
    msg match {
      case traced: TracedMessage[_] =>
        activeScope = Some(tracer.activateSpan(traced.activeSpan(tracer)))
        superAroundReceive(receive, traced.message)
      case _ =>
        superAroundReceive(receive, msg)
    }
  }

  override def afterReceive(receive: Receive, msg: Any): Unit = {
    activeScope.foreach(_.close())
    activeScope = None
  }
}

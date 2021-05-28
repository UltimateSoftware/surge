// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka

import akka.AroundReceiveActor
import io.opentracing.Scope
import org.slf4j.MDC
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
        traced.mdcContextMap.foreach(MDC.setContextMap)
        superAroundReceive(receive, traced.message)
      case _ =>
        superAroundReceive(receive, msg)
    }
  }

  override def afterReceive(receive: Receive, msg: Any): Unit = {
    activeScope.foreach(_.close())
    activeScope = None
    MDC.clear()
  }
}

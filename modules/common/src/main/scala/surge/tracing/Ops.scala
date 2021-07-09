// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.tracing

import akka.actor.ActorRef
import io.opentracing.{Span, Tracer}

trait ActorOps {
  implicit class ActorRefExtension(val actRef: ActorRef) {

    import SpanOps._

    def prettyPrintPath: String = actRef.path.toStringWithAddress(actRef.path.address)

    def tellAndTrace(msg: Any, spanToUse: Span)(implicit tracer: Tracer): Unit = {
      spanToUse.log(s"send", Map("destination path" -> actRef.prettyPrintPath,
        "message" -> msg.getClass.getName
      ))
      actRef ! TracedMessage(msg, spanToUse)
    }
  }
}

object ActorOps extends ActorOps


trait SpanOps {
  implicit class SpanExtension(val span: Span) {

    def log(event: String, fields: Map[String, String]): Span = {
      val scalaMap = fields + ("event" -> event)
      import scala.jdk.CollectionConverters._
      span.log(scalaMap.asJava)
    }
  }
}

object SpanOps extends SpanOps
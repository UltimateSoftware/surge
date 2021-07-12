// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.tracing

import akka.actor.ActorRef
import io.opentracing.{ Span, Tracer }

trait ActorOps {
  implicit class ActorRefExtension(val actRef: ActorRef) {

    import SpanOps._

    // pretty print actor path so we can include it in the OpenTracing annotations
    def prettyPrintPath: String = actRef.path.toStringWithAddress(actRef.path.address)

    def tellAndTrace(msg: TracedMessage, spanToUse: Span)(implicit tracer: Tracer): Unit = {
      spanToUse.log(s"send", Map("destination path" -> actRef.prettyPrintPath, "message" -> msg.getClass.getName))
      actRef ! msg
      spanToUse.finish()
    }
  }
}

object ActorOps extends ActorOps

trait SpanOps {
  implicit class SpanExtension(val span: Span) {

    /*
     * The purpose of this is to add a .log method on Span in order to make it more Scala friendly (i.e.
     * to be able to pass in a Scala map instead of a Java map).
     * @param eventName name of the event
     * @param fields fields as a Scala map
     * @return a span
     */
    def log(eventName: String, fields: Map[String, String] = Map.empty): Span = {
      val scalaMap = fields + ("event" -> eventName)
      import scala.jdk.CollectionConverters._
      span.log(scalaMap.asJava)
    }
  }
}

object SpanOps extends SpanOps

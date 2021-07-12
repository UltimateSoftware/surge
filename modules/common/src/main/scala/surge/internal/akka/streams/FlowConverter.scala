// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.streams

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Flow, GraphDSL, Merge, Partition }
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.{ Span, Tracer }
import io.opentelemetry.context.Context.root
import surge.internal.tracing.OpenTelemetryInstrumentation
import surge.streams.{ DataSinkExceptionHandler, EventPlusStreamMeta }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.hashing.MurmurHash3
import scala.util.{ Failure, Success }

object FlowConverter {

  // execute the handleEvent function with tracing
  private def executeBusinessLogic[K, V, Meta](sinkName: String)(evtPlusMeta: EventPlusStreamMeta[K, V, Meta], businessLogic: (K, V, Headers) => Future[Any])(
      implicit ec: ExecutionContext): Future[Any] = {
    import surge.internal.tracing.TracingHelper._
    val span = evtPlusMeta.span
    span.log("execute business logic", Map("sink class name" -> sinkName, "event" -> evtPlusMeta.messageBody.getClass.getSimpleName))
    val businessLogicFut: Future[Any] = businessLogic(evtPlusMeta.messageKey, evtPlusMeta.messageBody, evtPlusMeta.headers)
    businessLogicFut.transform {
      case failure @ Failure(exception) =>
        span.error(exception)
        span.end()
        failure
      case success @ Success(_) =>
        span.end()
        success
    }
  }

  type Headers = Map[String, Array[Byte]]
  def flowFor[K, V, Meta](
      sinkName: String,
      businessLogic: (K, V, Headers) => Future[Any],
      partitionBy: (K, V, Headers) => String,
      exceptionHandler: DataSinkExceptionHandler[K, V],
      parallelism: Int)(implicit ec: ExecutionContext): Flow[EventPlusStreamMeta[K, V, Meta], Meta, NotUsed] = {

    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      def toPartition: EventPlusStreamMeta[K, V, Meta] => Int = { t =>
        math.abs(MurmurHash3.stringHash(partitionBy(t.messageKey, t.messageBody, t.headers)) % parallelism)
      }
      val partition = builder.add(Partition[EventPlusStreamMeta[K, V, Meta]](parallelism, toPartition))
      val merge = builder.add(Merge[Meta](parallelism))
      val flow = Flow[EventPlusStreamMeta[K, V, Meta]].mapAsync(1) { evtPlusMeta: EventPlusStreamMeta[K, V, Meta] =>
        executeBusinessLogic(sinkName)(evtPlusMeta, businessLogic)
          .recover { case e =>
            exceptionHandler.handleException(evtPlusMeta, e)
          }
          .map(_ => evtPlusMeta.streamMeta)
      }

      for (_ <- 1 to parallelism) { partition ~> flow ~> merge }

      FlowShape[EventPlusStreamMeta[K, V, Meta], Meta](partition.in, merge.out)
    })
  }
}

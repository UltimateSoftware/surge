// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.streams

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Flow, GraphDSL, Merge, Partition }
import surge.streams.{ DataSinkExceptionHandler, EventPlusStreamMeta }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.hashing.MurmurHash3

object FlowConverter {
  type Headers = Map[String, Array[Byte]]
  def flowFor[K, V, Meta](
    businessLogic: (K, V, Headers) => Future[Any],
    partitionBy: (K, V, Headers) => String,
    exceptionHandler: DataSinkExceptionHandler[K, V],
    parallelism: Int)(implicit ec: ExecutionContext): Flow[EventPlusStreamMeta[K, V, Meta], Meta, NotUsed] = {

    Flow.fromGraph(
      GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._
        def toPartition: EventPlusStreamMeta[K, V, Meta] => Int = { t =>
          math.abs(MurmurHash3.stringHash(partitionBy(t.messageKey, t.messageBody, t.headers)) % parallelism)
        }
        val partition = builder.add(Partition[EventPlusStreamMeta[K, V, Meta]](parallelism, toPartition))
        val merge = builder.add(Merge[Meta](parallelism))
        val flow = Flow[EventPlusStreamMeta[K, V, Meta]].mapAsync(1) { evtPlusMeta =>
          businessLogic(evtPlusMeta.messageKey, evtPlusMeta.messageBody, evtPlusMeta.headers).recover {
            case e =>
              exceptionHandler.handleException(evtPlusMeta.messageKey, evtPlusMeta.messageBody, evtPlusMeta.streamMeta, e)
          }.map(_ => evtPlusMeta.streamMeta)
        }

        for (_ <- 1 to parallelism) { partition ~> flow ~> merge }

        FlowShape[EventPlusStreamMeta[K, V, Meta], Meta](partition.in, merge.out)
      })
  }
}

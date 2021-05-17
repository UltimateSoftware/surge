// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams

import akka.NotUsed
import akka.stream.scaladsl.Flow
import surge.internal.akka.streams.FlowConverter

import scala.concurrent.{ ExecutionContext, Future }

trait DataSinkExceptionHandler[K, V] {
  def handleException[Meta](key: K, value: V, streamMeta: Meta, exception: Throwable): Unit
}

trait DataHandler[Key, Value] {
  def dataHandler[Meta]: Flow[EventPlusStreamMeta[Key, Value, Meta], Meta, NotUsed]
}
trait DataSink[Key, Value] extends DataHandler[Key, Value] {
  def parallelism: Int = 8
  def handle(key: Key, value: Value, headers: Map[String, Array[Byte]]): Future[Any]
  def partitionBy(key: Key, value: Value, headers: Map[String, Array[Byte]]): String
  def sinkExceptionHandler: DataSinkExceptionHandler[Key, Value]

  override def dataHandler[Meta]: Flow[EventPlusStreamMeta[Key, Value, Meta], Meta, NotUsed] = {
    FlowConverter.flowFor(handle, partitionBy, sinkExceptionHandler, parallelism)(ExecutionContext.global)
  }
}

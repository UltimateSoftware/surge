// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams

import akka.NotUsed
import akka.stream.scaladsl.Flow
import surge.internal.akka.streams.FlowConverter

import scala.concurrent.{ ExecutionContext, Future }

object KafkaDataHandler {
  def from[Key, Value](genericHandler: DataHandler[Key, Value]): KafkaDataHandler[Key, Value] = new KafkaDataHandler[Key, Value] {
    override def dataHandler: Flow[EventPlusStreamMeta[Key, Value, KafkaStreamMeta], KafkaStreamMeta, NotUsed] =
      genericHandler.dataHandler[KafkaStreamMeta]
  }
}
trait KafkaDataHandler[Key, Value] {
  def dataHandler: Flow[EventPlusStreamMeta[Key, Value, KafkaStreamMeta], KafkaStreamMeta, NotUsed]
}
trait KafkaDataSink[Key, Value] extends KafkaDataHandler[Key, Value] {

  def sinkName: String = this.getClass.getSimpleName

  def parallelism: Int = 8
  def handle(key: Key, value: Value, headers: Map[String, Array[Byte]]): Future[Any]
  def partitionBy(key: Key, value: Value, headers: Map[String, Array[Byte]]): String
  def sinkExceptionHandler: DataSinkExceptionHandler[Key, Value]

  override def dataHandler: Flow[EventPlusStreamMeta[Key, Value, KafkaStreamMeta], KafkaStreamMeta, NotUsed] = {
    FlowConverter.flowFor(sinkName, handle, partitionBy, sinkExceptionHandler, parallelism)(ExecutionContext.global)
  }
}

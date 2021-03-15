// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.stream.alpakka.amqp.scaladsl.CommittableReadResult
import akka.util.ByteString
import surge.internal.utils.Logging
import surge.streams.{ DataPipeline, EventHandler, EventSourceDeserialization }

trait RabbitEventSource[Event] extends RabbitDataSource[String, Array[Byte]]
  with EventSourceDeserialization[Event]
  with Logging {

  override def readResultToKey: CommittableReadResult => String = { readResult => readResult.message.envelope.getRoutingKey }
  override def readResultToValue: ByteString => Array[Byte] = { byteString => byteString.toArray }

  def to(sink: EventHandler[Event]): DataPipeline = {
    super.to(dataHandler(sink))
  }
}


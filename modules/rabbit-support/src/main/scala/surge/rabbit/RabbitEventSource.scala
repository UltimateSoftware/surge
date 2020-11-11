// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.amqp.javadsl.{ AmqpSource, CommittableReadResult }
import akka.stream.alpakka.amqp.{ AmqpConnectionProvider, AmqpUriConnectionProvider, NamedQueueSourceSettings, QueueDeclaration }
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import akka.util.ByteString
import surge.akka.streams.graph.PassThroughFlow
import surge.core._
import surge.support.Logging

trait RabbitDataSource[Key, Value] extends DataSource {
  def actorSystem: ActorSystem

  def rabbitMqUri: String

  def queueName: String

  def readResultToKey: CommittableReadResult ⇒ Key

  def readResultToValue: ByteString ⇒ Value

  private val connectionProvider: AmqpConnectionProvider = AmqpUriConnectionProvider(rabbitMqUri)
  private val queueDeclaration = QueueDeclaration(queueName)

  private def businessFlow(sink: DataHandler[Key, Value]): Flow[CommittableReadResult, CommittableReadResult, NotUsed] = {
    val handleEventFlow = Flow[CommittableReadResult].map { crr ⇒
      readResultToKey(crr) -> readResultToValue(crr.message.bytes)
    }.via(sink.dataHandler)

    Flow[CommittableReadResult].via(PassThroughFlow(handleEventFlow, Keep.right))
  }

  def to(sink: DataHandler[Key, Value]): DataPipeline = {
    AmqpSource.committableSource(
      NamedQueueSourceSettings(connectionProvider, queueName)
        .withDeclaration(queueDeclaration), bufferSize = 10)
      .via(businessFlow(sink))
      .mapAsync(1, cm ⇒ cm.ack)
      .runWith(Sink.ignore, actorSystem)

    new RabbitDataPipeline()
  }
}

trait RabbitEventSource[Event] extends RabbitDataSource[String, Array[Byte]]
  with EventSourceDeserialization[Event]
  with Logging {

  override def readResultToKey: CommittableReadResult ⇒ String = { readResult ⇒ readResult.message.envelope.getRoutingKey }
  override def readResultToValue: ByteString ⇒ Array[Byte] = { byteString ⇒ byteString.toArray }

  def to(sink: EventHandler[Event]): DataPipeline = {
    super.to(dataHandler(sink))
  }
}


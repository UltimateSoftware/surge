// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import surge.akka.cluster.JacksonSerializable
import surge.tracing.TracedMessage

trait RoutableMessage extends JacksonSerializable {
  def aggregateId: String
}
object RoutableMessage {
  def extractEntityId: PartialFunction[Any, String] = {
    case routableMessage: RoutableMessage       => routableMessage.aggregateId
    case traced: TracedMessage[RoutableMessage] => traced.message.aggregateId
  }
}

trait SurgePersistentEntity[BaseMsg <: RoutableMessage, Model] {
  def receiveMessage: PartialFunction[Any, Unit]
}

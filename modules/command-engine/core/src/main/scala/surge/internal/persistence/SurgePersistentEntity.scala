// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import surge.akka.cluster.JacksonSerializable

trait RoutableMessage extends JacksonSerializable {
  def aggregateId: String
}
object RoutableMessage {
  def extractEntityId: PartialFunction[Any, String] = { case routableMessage: RoutableMessage =>
    routableMessage.aggregateId
  }
}

trait SurgePersistentEntity[BaseMsg <: RoutableMessage, Model] {
  def receiveMessage: PartialFunction[Any, Unit]
}

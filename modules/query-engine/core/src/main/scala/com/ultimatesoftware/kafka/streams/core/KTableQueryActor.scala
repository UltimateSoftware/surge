// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor.{ Actor, Address, Props }
import akka.pattern._
import akka.util.Timeout
import com.ultimatesoftware.akka.cluster.ActorHostAwareness
import com.ultimatesoftware.kafka.streams.KafkaStreamsKeyValueStore
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams

import scala.concurrent.duration._

object KTableQueryActor {
  def props[A](streams: KafkaStreams, storeName: String, keyValueStore: KafkaStreamsKeyValueStore[String, A]): Props = {
    Props(new KTableQueryActor[A](streams, storeName, keyValueStore))
  }

  sealed trait AggregateQuery {
    def aggregateId: String
  }
  sealed trait AggregateResponse
  case class GetState(aggregateId: String) extends AggregateQuery
  case class FetchedState[A](aggregateId: String, state: Option[A]) extends AggregateResponse
}

class KTableQueryActor[A](streams: KafkaStreams, storeName: String, keyValueStore: KafkaStreamsKeyValueStore[String, A]) extends Actor with ActorHostAwareness {
  import KTableQueryActor._
  import context.dispatcher

  override def receive: Receive = {
    case msg: GetState ⇒ handleGetState(msg.aggregateId)
  }

  private def handleGetState(aggregateId: String): Unit = {
    val aggregateOwnerHostInfo = streams.metadataForKey(storeName, aggregateId, new StringSerializer()).hostInfo()

    val fetchedStateFuture = if (isHostInfoThisNode(aggregateOwnerHostInfo)) {
      // State for key is local, just query state store
      keyValueStore.get(aggregateId).map(aggregateOpt ⇒ FetchedState(aggregateId, aggregateOpt))
    } else {
      // State for key is remote, ask peer who owns the aggregate for an answer
      val remoteAddress = Address(akkaProtocol, context.system.name, aggregateOwnerHostInfo.host, aggregateOwnerHostInfo.port)
      val routerActorRemoteNode = self.path.toStringWithAddress(remoteAddress)
      implicit val askTimeout: Timeout = Timeout(15.seconds)

      context.actorSelection(routerActorRemoteNode) ? GetState(aggregateId)
    }

    fetchedStateFuture pipeTo sender()
  }

}

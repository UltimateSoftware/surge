// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import akka.actor.ActorSystem
import akka.pattern._
import akka.util.Timeout
import com.ultimatesoftware.akka.cluster.RemoteAddressExtension
import com.ultimatesoftware.kafka.streams.core.{ KTableQueryActor, KafkaStreamsEventProcessor }
import com.ultimatesoftware.scala.core.domain.StateMessage
import com.ultimatesoftware.scala.core.messaging.EventProperties
import play.api.libs.json.Format

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

object KafkaStreamsQuery {
  def apply[AggId, Agg, Event, EvtMeta <: EventProperties](
    businessLogic: KafkaStreamsQueryBusinessLogic[AggId, Agg, Event, EvtMeta])(implicit format: Format[Agg]): KafkaStreamsQuery[AggId, Agg, Event, EvtMeta] = {

    val actorSystem = ActorSystem(s"${businessLogic.aggregateName}ActorSystem")
    new KafkaStreamsQuery(actorSystem, businessLogic)
  }
}

class KafkaStreamsQuery[AggId, Agg, Event, EvtMeta <: EventProperties] private (
    val actorSystem: ActorSystem,
    businessLogic: KafkaStreamsQueryBusinessLogic[AggId, Agg, Event, EvtMeta])(implicit format: Format[Agg]) {

  private val akkaAddress = RemoteAddressExtension(actorSystem).address
  private val akkaHost = akkaAddress.host.getOrElse(throw new RuntimeException("Unable to determine hostname of current Akka actor system"))
  private val akkaPort = akkaAddress.port.getOrElse(throw new RuntimeException("Unable to determine port of current Akka actor system"))
  private val applicationHostPort = s"$akkaHost:$akkaPort"

  val aggregateStreamsFromEvents: KafkaStreamsEventProcessor[AggId, Agg, Event, EvtMeta] = new KafkaStreamsEventProcessor[AggId, Agg, Event, EvtMeta](
    aggregateName = businessLogic.aggregateName,
    aggregateTypeInfo = businessLogic.aggregateTypeInfo,
    readFormatting = businessLogic.readFormatting,
    writeFormatting = businessLogic.writeFormatting,
    aggIdExtractor = businessLogic.extractAggId,
    eventsTopic = businessLogic.eventsTopic,
    applicationHostPort = Some(applicationHostPort),
    processEvent = businessLogic.eventProcessor.handleEvent)

  private lazy val queryActorProps = KTableQueryActor.props(aggregateStreamsFromEvents.consumer.streams, aggregateStreamsFromEvents.aggregateKTableStoreName,
    aggregateStreamsFromEvents.aggregateQueryableStateStore)
  private lazy val queryActor = actorSystem.actorOf(queryActorProps, "queryActor")

  def getAggregateState(aggregateId: String)(implicit ec: ExecutionContext): Future[Option[Agg]] = {
    implicit val timeout: Timeout = Timeout(15.seconds)
    (queryActor ? KTableQueryActor.GetState(aggregateId)).map {
      case fetched: KTableQueryActor.FetchedState[Array[Byte]] ⇒
        fetched.state.flatMap { aggBytes ⇒
          businessLogic.readFormatting.readState(aggBytes).flatMap { segment ⇒
            segment.value.asOpt[StateMessage[Agg]].flatMap(_.body)
          }
        }
    }
  }
}

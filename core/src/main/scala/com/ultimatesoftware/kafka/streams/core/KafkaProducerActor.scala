// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Stash }
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, GlobalKTableMetadataHandler, KafkaPartitionMetadata }
import com.ultimatesoftware.scala.core.kafka._
import com.ultimatesoftware.scala.core.messaging.{ EventMessage, StateMessage }
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, Rate, Timer }
import com.ultimatesoftware.scala.core.utils.JsonUtils
import org.apache.kafka.clients.producer.{ ProducerConfig, ProducerRecord }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ProducerFencedException
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.JsValue

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class KafkaProducerActor[Agg, AggIdType, Event](
    actorSystem: ActorSystem,
    assignedPartition: TopicPartition,
    metricsProvider: MetricsProvider,
    stateMetaHandler: GlobalKTableMetadataHandler,
    aggregateCommandKafkaStreams: KafkaStreamsCommandBusinessLogic[Agg, AggIdType, _, Event, _],
    kafkaStreamsImpl: AggregateStateStoreKafkaStreams) {
  import aggregateCommandKafkaStreams.businessLogicAdapter._

  private val log = LoggerFactory.getLogger(getClass)

  private val publisherActor = actorSystem.actorOf(
    Props(new KafkaProducerActorImpl(
      assignedPartition, metricsProvider, stateMetaHandler, aggregateCommandKafkaStreams, kafkaStreamsImpl)).withDispatcher("kafka-publisher-actor-dispatcher"))

  private val publishEventsTimer: Timer = metricsProvider.createTimer(s"${aggregateName}PublishEventsTimer")
  def publish(aggregateId: AggIdType, states: Seq[StateMessage[JsValue]], events: Seq[EventMessage[Event]]): Future[Done] = {

    val eventKeyValuePairs = events.map { event ⇒
      log.trace(s"Publishing event for $aggregateName $aggregateId")
      s"${event.aggregateId}:${event.sequenceNumber}" -> JsonUtils.gzip(event)
    }
    val stateKeyValuePairs = states.flatMap { msg ⇒
      msg.body.map { _ ⇒
        log.trace(s"Publishing state for $aggregateName $aggregateId")
        msg.fullIdentifier -> JsonUtils.gzip(msg)
      } orElse {
        log.warn(s"Tried to publish StateMessage with no state. This shouldn't happen")
        None
      }
    }

    publishEventsTimer.time {
      implicit val askTimeout: Timeout = Timeout(5.seconds)
      (publisherActor ? KafkaProducerActorImpl.Publish(eventKeyValuePairs = eventKeyValuePairs, stateKeyValuePairs = stateKeyValuePairs))
        .map(_ ⇒ Done)(ExecutionContext.global)
    }
  }

  private val isAggregateStateCurrentTimer: Timer = metricsProvider.createTimer(s"${aggregateName}IsAggregateCurrentTimer")
  def isAggregateStateCurrent(aggregateId: String): Future[Boolean] = {
    implicit val askTimeout: Timeout = Timeout(10.seconds)
    val expirationTime = Instant.now.plusSeconds(4)
    isAggregateStateCurrentTimer.time {
      (publisherActor ? KafkaProducerActorImpl.IsAggregateStateCurrent(aggregateId, expirationTime)).mapTo[Boolean]
    }
  }
}

private object KafkaProducerActorImpl {
  case class Publish(stateKeyValuePairs: Seq[(String, Array[Byte])], eventKeyValuePairs: Seq[(String, Array[Byte])])
  case class StateProcessed(stateMeta: KafkaPartitionMetadata)
  case class IsAggregateStateCurrent(aggregateId: String, expirationTime: Instant)
}
private class KafkaProducerActorImpl[Agg, Event](
    assignedPartition: TopicPartition, metrics: MetricsProvider,
    stateMetaHandler: GlobalKTableMetadataHandler,
    aggregateCommandKafkaStreams: KafkaStreamsCommandBusinessLogic[Agg, _, _, Event, _],
    kafkaStreamsImpl: AggregateStateStoreKafkaStreams) extends Actor with Stash {

  import KafkaProducerActorImpl._
  import context.dispatcher
  import aggregateCommandKafkaStreams._
  import aggregateCommandKafkaStreams.businessLogicAdapter._

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private val config = ConfigFactory.load()
  private val assignedTopicPartitionKey = s"${assignedPartition.topic}:${assignedPartition.partition}"
  private val flushInterval = config.getDuration("kafka.publisher.flush-interval", TimeUnit.MILLISECONDS).milliseconds
  private val brokers = config.getString("kafka.brokers").split(",")

  private val kafkaPublisher: KafkaBytesProducer = {
    val transactionalId = s"$aggregateName-event-producer-partition-${assignedPartition.partition()}"
    val kafkaConfig = Map[String, String](
      ProducerConfig.TRANSACTIONAL_ID_CONFIG -> transactionalId)

    KafkaBytesProducer(brokers, stateTopic, partitioner, kafkaConfig)
  }

  private val nonTransactionalStatePublisher = KafkaBytesProducer(brokers, stateTopic, partitioner = partitioner)

  private val kafkaPublisherTimer: Timer = metrics.createTimer(s"${stateTopic.name}KafkaPublisherTimer")
  private val aggregateStateNotCurrentRate: Rate = metrics.createRate(
    s"${aggregateName}AggregateStateNotCurrentRate")
  private val aggregateStateCurrentRate: Rate = metrics.createRate(s"${aggregateName}AggregateStateCurrentRate")

  private sealed trait InternalMessage
  private case class EventsPublished(originalSenders: Seq[ActorRef], recordMetadata: Seq[KafkaRecordMetadata[String]]) extends InternalMessage
  private case class Initialize(endOffset: Long) extends InternalMessage
  private case object FailedToInitialize extends InternalMessage
  private case object FlushMessages extends InternalMessage
  private case class PublishWithSender(sender: ActorRef, publish: Publish) extends InternalMessage
  private case class PendingInitialization(actor: ActorRef, key: String, expiration: Instant) extends InternalMessage

  // TODO optimize:
  //  Only keep last offset for an aggregate
  //  Add in a warning if state gets too large
  private object ActorState {
    def empty: ActorState = ActorState(Seq.empty, Seq.empty, Seq.empty, transactionInProgress = false)
  }
  private case class ActorState(inFlight: Seq[KafkaRecordMetadata[String]], pendingWrites: Seq[PublishWithSender],
      pendingInitializations: Seq[PendingInitialization], transactionInProgress: Boolean) {

    def inFlightByKey: Map[String, Seq[KafkaRecordMetadata[String]]] = {
      inFlight.groupBy(_.key.getOrElse(""))
    }

    def inFlightForAggregate(aggregateId: String): Seq[KafkaRecordMetadata[String]] = {
      inFlightByKey.getOrElse(aggregateId, Seq.empty)
    }

    def addPendingInitialization(sender: ActorRef, isAggregateStateCurrent: IsAggregateStateCurrent): ActorState = {
      val pendingInitialization = PendingInitialization(sender, isAggregateStateCurrent.aggregateId, isAggregateStateCurrent.expirationTime)

      this.copy(pendingInitializations = pendingInitializations :+ pendingInitialization)
    }

    def addPendingWrites(sender: ActorRef, publish: Publish): ActorState = {
      val newWriteRequest = PublishWithSender(sender, publish)
      this.copy(pendingWrites = pendingWrites :+ newWriteRequest)
    }

    def flushWrites(): ActorState = {
      this.copy(pendingWrites = Seq.empty)
    }

    def addInFlight(recordMetadata: Seq[KafkaRecordMetadata[String]]): ActorState = {
      val newTotalInFlight = inFlight ++ recordMetadata
      val newInFlight = newTotalInFlight.groupBy(_.key).mapValues(_.maxBy(_.wrapped.offset())).values

      this.copy(inFlight = newInFlight.toSeq)
    }

    def processedUpTo(stateMeta: KafkaPartitionMetadata): ActorState = {
      val processedRecordsFromPartition = inFlight.filter(record ⇒ record.wrapped.offset() <= stateMeta.offset)

      if (processedRecordsFromPartition.nonEmpty) {
        val processedOffsets = processedRecordsFromPartition.map(_.wrapped.offset())
        log.debug(s"${stateMeta.topic}:${stateMeta.partition} processed up to offset ${stateMeta.offset}. " +
          s"Outstanding offsets that were processed are [${processedOffsets.min} -> ${processedOffsets.max}]")
      }
      val newInFlight = inFlight.filterNot(processedRecordsFromPartition.contains)

      val newPendingAggregates = {
        val processedAggregates = pendingInitializations.filter { pending ⇒
          !newInFlight.exists(_.key.contains(pending.key))
        }
        if (processedAggregates.nonEmpty) {
          processedAggregates.foreach { pending ⇒
            pending.actor ! true
          }
          aggregateStateCurrentRate.mark(processedAggregates.length)
        }

        val expiredAggregates = pendingInitializations
          .filter(pending ⇒ Instant.now().isAfter(pending.expiration))
          .filterNot(processedAggregates.contains)

        if (expiredAggregates.nonEmpty) {
          expiredAggregates.foreach { pending ⇒
            log.debug(s"Aggregate ${pending.key} expiring since it is past ${pending.expiration}")
            pending.actor ! false
          }
          aggregateStateNotCurrentRate.mark(expiredAggregates.length)
        }
        pendingInitializations.filterNot(agg ⇒ processedAggregates.contains(agg) || expiredAggregates.contains(agg))
      }

      copy(inFlight = newInFlight, pendingInitializations = newPendingAggregates)
    }
  }

  context.system.scheduler.scheduleOnce(10.milliseconds)(initializeState())
  context.system.scheduler.schedule(200.milliseconds, 200.milliseconds)(refreshStateMeta())
  context.system.scheduler.schedule(flushInterval, flushInterval, self, FlushMessages)

  override def receive: Receive = uninitialized

  private def uninitialized: Receive = {
    case msg: Initialize    ⇒ handle(msg)
    case FailedToInitialize ⇒ context.system.scheduler.scheduleOnce(3.seconds)(initializeState())
    case FlushMessages      ⇒ // Ignore from this state
    case _                  ⇒ stash()
  }

  private def recoveringBacklog(endOffset: Long): Receive = {
    case msg: StateProcessed ⇒ handleFromRecoveringState(endOffset, msg)
    case FlushMessages       ⇒ // Ignore from this state
    case _                   ⇒ stash()
  }

  private def processing(state: ActorState): Receive = {
    case msg: Publish                 ⇒ handle(state, msg)
    case msg: EventsPublished         ⇒ handle(state, msg)
    case msg: StateProcessed          ⇒ handle(state, msg)
    case msg: IsAggregateStateCurrent ⇒ handle(state, msg)
    case FlushMessages                ⇒ handleFlushMessages(state)
  }

  private def initializeState(): Unit = {
    val futureInitializeMessage = kafkaPublisher.initTransactions().flatMap { _ ⇒
      val emptyEventMessage = JsonUtils.gzip(StateMessage.empty[JsValue])
      val flushRecord = new ProducerRecord[String, Array[Byte]](assignedPartition.topic(), assignedPartition.partition(), "", emptyEventMessage)

      nonTransactionalStatePublisher.putRecord(flushRecord).map { flushRecordMeta ⇒
        val flushRecordOffset = flushRecordMeta.wrapped.offset()
        log.debug("Flush Record for partition {} is at offset {}", assignedPartition, flushRecordOffset)

        Initialize(flushRecordOffset)
      } recover {
        case e ⇒
          log.error("Failed to initialize kafka producer actor state", e)
          FailedToInitialize
      }
    }
    futureInitializeMessage pipeTo self
  }

  private def refreshStateMeta(): Unit = {
    stateMetaHandler.stateMetaQueryableStore.get(assignedTopicPartitionKey).map {
      case Some(meta) ⇒
        self ! StateProcessed(meta)
      case _ ⇒
        log.warn(s"No state metadata found for $assignedPartition")
        val meta = KafkaPartitionMetadata(assignedPartition.topic, assignedPartition.partition, offset = -1L, key = "")
        self ! StateProcessed(meta)
    } recover {
      case e ⇒
        log.error(s"Failed to fetch state metadata for $assignedPartition", e)
    }
  }

  private def handle(initialize: Initialize): Unit = {
    log.debug(s"Publisher actor initializing for topic-partition $assignedPartition with end offset ${initialize.endOffset}")
    context.become(recoveringBacklog(initialize.endOffset))
  }

  private def handle(state: ActorState, publish: Publish): Unit = {
    context.become(processing(state.addPendingWrites(sender(), publish)))
  }

  private def handle(state: ActorState, stateProcessed: StateProcessed): Unit = {
    context.become(processing(state.processedUpTo(stateProcessed.stateMeta)))
    sender() ! Done
  }

  private def handleFromRecoveringState(endOffset: Long, stateProcessed: StateProcessed): Unit = {
    val stateMeta = stateProcessed.stateMeta

    log.trace("KafkaPublisherActor partition {} received StateMeta {}", Seq(assignedPartition, stateMeta): _*)
    val partitionIsCurrent = assignedPartition.topic() == stateMeta.topic &&
      assignedPartition.partition() == stateMeta.partition &&
      endOffset <= stateMeta.offset

    if (partitionIsCurrent) {
      log.info(s"KafkaPublisherActor partition {} is no longer behind on processing", assignedPartition)
      unstashAll()
      context.become(processing(ActorState.empty))
    }
  }

  private def handle(state: ActorState, eventsPublished: EventsPublished): Unit = {
    val newState = state.addInFlight(eventsPublished.recordMetadata).copy(transactionInProgress = false)
    context.become(processing(newState))
    eventsPublished.originalSenders.foreach(_ ! Done)
  }

  private val eventsPublishedRate: Rate = metrics.createRate(s"${aggregateName}EventPublishRate")
  private def handleFlushMessages(state: ActorState): Unit = {
    if (state.transactionInProgress) {
      log.warn(s"KafkaPublisherActor partition $assignedPartition tried to flush, but another transaction is already in progress")
    } else if (state.pendingWrites.nonEmpty) {
      val senders = state.pendingWrites.map(_.sender)
      val eventMessages = state.pendingWrites.flatMap(_.publish.eventKeyValuePairs)
      val stateMessages = state.pendingWrites.flatMap(_.publish.stateKeyValuePairs)

      val eventRecords = eventMessages.map { tup ⇒
        new ProducerRecord(eventsTopic.name, tup._1, tup._2)
      }
      val stateRecords = stateMessages.map { tup ⇒
        new ProducerRecord(stateTopic.name, tup._1, tup._2)
      }
      val records = eventRecords ++ stateRecords

      log.trace(s"KafkaPublisherActor partition {} writing {} events to Kafka", assignedPartition, eventRecords.length)
      log.trace(s"KafkaPublisherActor partition {} writing {} states to Kafka", assignedPartition, stateRecords.length)
      eventsPublishedRate.mark(eventMessages.length)
      val futureMsg = kafkaPublisherTimer.time {
        kafkaPublisher.beginTransaction()
        Future.sequence(kafkaPublisher.putRecords(records)).map { recordMeta ⇒
          log.trace(s"KafkaPublisherActor partition {} committing transaction", assignedPartition)
          kafkaPublisher.commitTransaction()
          EventsPublished(senders, recordMeta.filter(_.wrapped.topic() == stateTopic.name))
        } recover {
          case _: ProducerFencedException ⇒
            log.error(s"KafkaPublisherActor partition $assignedPartition tried to commit a transaction, but was fenced out by another producer instance.")
            kafkaPublisher.abortTransaction()
            context.stop(self)
          case e ⇒
            log.error(s"KafkaPublisherActor partition $assignedPartition got error while trying to publish to Kafka", e)
            kafkaPublisher.abortTransaction()
            throw e
        }
      }

      context.become(processing(state.flushWrites().copy(transactionInProgress = true)))
      futureMsg.pipeTo(self)(sender())
    }
  }

  private def handle(state: ActorState, isAggregateStateCurrent: IsAggregateStateCurrent): Unit = {
    val aggregateId = isAggregateStateCurrent.aggregateId
    val noRecordsInFlight = state.inFlightForAggregate(aggregateId).isEmpty

    if (noRecordsInFlight) {
      sender() ! noRecordsInFlight
    } else {
      context.become(processing(state.addPendingInitialization(sender(), isAggregateStateCurrent)))
    }
  }
}

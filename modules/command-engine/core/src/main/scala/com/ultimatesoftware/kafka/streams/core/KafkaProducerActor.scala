// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Stash }
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.config.TimeoutConfig
import com.ultimatesoftware.kafka.streams.HealthyActor.GetHealth
import com.ultimatesoftware.kafka.streams._
import com.ultimatesoftware.scala.core.domain.StateMessage
import com.ultimatesoftware.scala.core.kafka._
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, Rate, Timer }
import com.ultimatesoftware.scala.core.utils.JsonUtils
import com.ultimatesoftware.scala.oss.domain.AggregateSegment
import org.apache.kafka.clients.producer.{ ProducerConfig, ProducerRecord }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ProducerFencedException
import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Try }

/**
 * A stateful producer actor responsible for publishing all states + events for
 * aggregates that belong to a particular state topic partition.  The state maintained
 * by this producer actor is a list of aggregate ids which are considered "in flight".
 * "In flight" is determined by keeping track of the offset this actor publishes to for
 * each aggregate id as messages are published to Kafka and listening to updates of the downstream
 * Kafka Streams consumer as it makes progress through the topic.  As a state is published,
 * this actor remembers the aggregate id and offset the state for that aggregate is.  When the
 * Kafka Streams consumer processes the state (saving it to a KTable) it saves the offset of the most
 * recently processed message for a partition to a global KTable.  This producer actor polls that global
 * table to get the last processed offset and marks any aggregates in the "in flight" state as up to date
 * if their offset is less than or equal to the last processed offset.
 *
 * When an aggregate actor wants to initialize, it must first ask this stateful producer if
 * the state for that aggregate is up to date in the Kafka Streams state store KTable.  The
 * stateful producer is able to determine this by looking at the aggregates with states that
 * are in flight - if any are in flight for an aggregate, the state in the KTable is not up to
 * date and initialization of that actor should be delayed.
 *
 * On initialization of the stateful producer, it emits an empty "flush" record to the Kafka
 * state topic.  The flush record is for an empty aggregate id, but is used to ensure on initial startup
 * that there were no remaining aggregate states that were in flight, since the newly created
 * producer cannot initialize with the knowledge of everything that was published previously.
 *
 * @param actorSystem The actor system to create the underlying stateful producer actor in
 * @param assignedPartition The state topic/partition assigned to this instance of the stateful producer.
 *                          The producer will use Kafka transactions to ensure that it is the only instance
 *                          of a stateful producer for a particular partition.  Any older producers for
 *                          that partition will be fenced out by Kafka.
 * @param metricsProvider Metrics provider interface to use for recording internal metrics to
 * @param stateMetaHandler An instance of the Kafka Streams partition metadata global KTable processor that
 *                         is responsible for tracking what offsets in Kafka have been processed by the aggregate
 *                         state indexer process.  The producer will query the underlying global KTable to determine
 *                         which aggregates are in flight.
 * @param aggregateCommandKafkaStreams Command service business logic wrapper used for determining state and event topics
 * @tparam AggId Generic aggregate id type for aggregates publishing states/events through this stateful producer
 * @tparam Agg Generic aggregate type of aggregates publishing states/events through this stateful producer
 * @tparam Event Generic base type for events that aggregate instances publish through this stateful producer
 */
class KafkaProducerActor[AggId, Agg, Event, EvtMeta](
    actorSystem: ActorSystem,
    assignedPartition: TopicPartition,
    metricsProvider: MetricsProvider,
    stateMetaHandler: GlobalKTableMetadataHandler,
    aggregateCommandKafkaStreams: KafkaStreamsCommandBusinessLogic[AggId, Agg, _, Event, _, EvtMeta]) extends HealthyComponent {

  private val log = LoggerFactory.getLogger(getClass)
  private val aggregateName: String = aggregateCommandKafkaStreams.aggregateName

  private val publisherActor = actorSystem.actorOf(
    Props(new KafkaProducerActorImpl(
      assignedPartition, metricsProvider, stateMetaHandler, aggregateCommandKafkaStreams)).withDispatcher("kafka-publisher-actor-dispatcher"))

  private val publishEventsTimer: Timer = metricsProvider.createTimer(s"${aggregateName}PublishEventsTimer")
  def publish(aggregateId: AggId, states: Seq[(String, AggregateSegment[AggId, Agg])], events: Seq[(String, EvtMeta, Event)]): Future[Done] = {

    val eventKeyValuePairs = events.map { eventTuple ⇒
      log.trace(s"Publishing event for $aggregateName $aggregateId")
      eventTuple._1 -> aggregateCommandKafkaStreams.writeFormatting.writeEvent(eventTuple._3, eventTuple._2)
    }
    val stateKeyValuePairs = states.map { stateKv ⇒
      log.trace(s"Publishing state for $aggregateName $aggregateId")
      stateKv._1 -> aggregateCommandKafkaStreams.writeFormatting.writeState(stateKv._2)
    }

    publishEventsTimer.time {
      implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PublisherActor.publishTimeout)
      (publisherActor ? KafkaProducerActorImpl.Publish(eventKeyValuePairs = eventKeyValuePairs, stateKeyValuePairs = stateKeyValuePairs))
        .map(_ ⇒ Done)
    }
  }

  private val isAggregateStateCurrentTimer: Timer = metricsProvider.createTimer(s"${aggregateName}IsAggregateCurrentTimer")
  def isAggregateStateCurrent(aggregateId: String): Future[Boolean] = {
    implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PublisherActor.aggregateStateCurrentTimeout)
    val expirationTime = Instant.now.plusMillis(askTimeout.duration.toMillis)
    isAggregateStateCurrentTimer.time {
      (publisherActor ? KafkaProducerActorImpl.IsAggregateStateCurrent(aggregateId, expirationTime)).mapTo[Boolean]
    }
  }

  def healthCheck(): Future[HealthCheck] = {
    publisherActor.ask(HealthyActor.GetHealth)(1.second)
      .mapTo[HealthCheck]
      .recoverWith {
        case err: Throwable ⇒
          log.error(s"Failed to get publisher-actor health check ${err.getMessage}")
          Future.successful(HealthCheck(
            name = "publisher-actor",
            id = aggregateName,
            status = HealthCheckStatus.DOWN))
      }
  }
}

// TODO: evaluate access modifier here, it was private
private object KafkaProducerActorImpl {
  sealed trait KafkaProducerActorMessage
  case class Publish(stateKeyValuePairs: Seq[(String, Array[Byte])], eventKeyValuePairs: Seq[(String, Array[Byte])]) extends KafkaProducerActorMessage
  case class StateProcessed(stateMeta: KafkaPartitionMetadata) extends KafkaProducerActorMessage
  case class IsAggregateStateCurrent(aggregateId: String, expirationTime: Instant) extends KafkaProducerActorMessage
  case class AggregateStateRates(current: Rate, notCurrent: Rate) extends KafkaProducerActorMessage

  sealed trait InternalMessage
  case class EventsPublished(originalSenders: Seq[ActorRef], recordMetadata: Seq[KafkaRecordMetadata[String]]) extends InternalMessage
  case object EventsFailedToPublish extends InternalMessage
  case object InitTransactions extends InternalMessage
  case object FailedToInitTransactions extends InternalMessage
  case class Initialize(endOffset: Long) extends InternalMessage
  case object FailedToInitialize extends InternalMessage
  case object FlushMessages extends InternalMessage
  case class PublishWithSender(sender: ActorRef, publish: Publish) extends InternalMessage
  case class PendingInitialization(actor: ActorRef, key: String, expiration: Instant) extends InternalMessage
}
private class KafkaProducerActorImpl[Agg, Event, EvtMeta](
    assignedPartition: TopicPartition, metrics: MetricsProvider,
    stateMetaHandler: GlobalKTableMetadataHandler,
    aggregateCommandKafkaStreams: KafkaStreamsCommandBusinessLogic[_, Agg, _, Event, _, _],
    kafkaProducerOverride: Option[KafkaBytesProducer] = None) extends Actor with Stash {

  import KafkaProducerActorImpl._
  import aggregateCommandKafkaStreams._
  import kafka._

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private val config = ConfigFactory.load()
  private val assignedTopicPartitionKey = s"${assignedPartition.topic}:${assignedPartition.partition}"
  private val flushInterval = config.getDuration("kafka.publisher.flush-interval", TimeUnit.MILLISECONDS).milliseconds
  private val brokers = config.getString("kafka.brokers").split(",")

  private val kafkaPublisher = kafkaProducerOverride.getOrElse({
    val transactionalId = s"$aggregateName-event-producer-partition-${assignedPartition.partition()}"
    val kafkaConfig = Map[String, String](
      // ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true, // For exactly once writes. How does this impact performance?
      ProducerConfig.TRANSACTIONAL_ID_CONFIG -> transactionalId)

    KafkaBytesProducer(brokers, stateTopic, partitioner, kafkaConfig)
  })

  private val nonTransactionalStatePublisher = kafkaProducerOverride.getOrElse(KafkaBytesProducer(brokers, stateTopic, partitioner = partitioner))

  private val kafkaPublisherTimer: Timer = metrics.createTimer(s"${stateTopic.name}KafkaPublisherTimer")
  private implicit val rates: AggregateStateRates = AggregateStateRates(
    current = metrics.createRate(s"${aggregateName}AggregateStateCurrentRate"),
    notCurrent = metrics.createRate(s"${aggregateName}AggregateStateNotCurrentRate"))

  context.system.scheduler.scheduleOnce(10.milliseconds, self, InitTransactions)
  context.system.scheduler.schedule(200.milliseconds, 200.milliseconds)(refreshStateMeta())
  context.system.scheduler.schedule(flushInterval, flushInterval, self, FlushMessages)

  override def receive: Receive = uninitialized

  private def uninitialized: Receive = {
    case InitTransactions           ⇒ initializeTransactions()
    case msg: Initialize            ⇒ handle(msg)
    case FailedToInitialize         ⇒ context.system.scheduler.scheduleOnce(3.seconds)(initializeState())
    case FlushMessages              ⇒ // Ignore from this state
    case GetHealth                  ⇒ getHealthCheck()
    case _: Publish                 ⇒ stash()
    case _: StateProcessed          ⇒ stash()
    case _: IsAggregateStateCurrent ⇒ stash()
    case unknown                    ⇒ log.warn("Receiving unhandled message on uninitialized state", unknown)
  }

  private def recoveringBacklog(endOffset: Long): Receive = {
    case msg: StateProcessed        ⇒ handleFromRecoveringState(endOffset, msg)
    case FlushMessages              ⇒ // Ignore from this state
    case GetHealth                  ⇒ getHealthCheck()
    case _: Publish                 ⇒ stash()
    case _: IsAggregateStateCurrent ⇒ stash()
    case unknown                    ⇒ log.warn("Receiving unhandled message on recoveringBacklog state", unknown)
  }

  private def processing(state: KafkaProducerActorState): Receive = {
    case msg: Publish                 ⇒ handle(state, msg)
    case msg: EventsPublished         ⇒ handle(state, msg)
    case EventsFailedToPublish        ⇒ handleFailedToPublish(state)
    case msg: StateProcessed          ⇒ handle(state, msg)
    case msg: IsAggregateStateCurrent ⇒ handle(state, msg)
    case GetHealth                    ⇒ getHealthCheck(state)
    case FlushMessages                ⇒ handleFlushMessages(state)
  }

  private def sendFlushRecord: Future[InternalMessage] = {
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

  private def initializeTransactions(): Unit = {
    kafkaPublisher.initTransactions().map { _ ⇒
      log.debug(s"KafkaPublisherActor transactions successfully initialized: $assignedPartition")
      initializeState()
    }.recover {
      case err: Throwable ⇒
        log.error(s"KafkaPublisherActor failed to initialize kafka transactions, retrying in 3 seconds: $assignedPartition $err")
        context.system.scheduler.scheduleOnce(3.seconds, self, InitTransactions)
    }
  }

  private def initializeState(): Unit = {
    sendFlushRecord pipeTo self
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

  private def handle(state: KafkaProducerActorState, publish: Publish): Unit = {
    context.become(processing(state.addPendingWrites(sender(), publish)))
  }

  private def handle(state: KafkaProducerActorState, stateProcessed: StateProcessed): Unit = {
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
      log.info(s"KafkaPublisherActor partition {} is fully up to date on processing", assignedPartition)
      unstashAll()
      context.become(processing(KafkaProducerActorState.empty))
    }
  }

  private def handle(state: KafkaProducerActorState, eventsPublished: EventsPublished): Unit = {
    val newState = state.addInFlight(eventsPublished.recordMetadata).copy(transactionInProgress = false)
    context.become(processing(newState))
    eventsPublished.originalSenders.foreach(_ ! Done)
  }

  private def handleFailedToPublish(state: KafkaProducerActorState): Unit = {
    val newState = state.copy(transactionInProgress = false)
    context.become(processing(newState))
  }

  private val eventsPublishedRate: Rate = metrics.createRate(s"${aggregateName}EventPublishRate")
  private def handleFlushMessages(state: KafkaProducerActorState): Unit = {
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

      log.info(s"KafkaPublisherActor partition {} writing {} events to Kafka", assignedPartition, eventRecords.length)
      log.info(s"KafkaPublisherActor partition {} writing {} states to Kafka", assignedPartition, stateRecords.length)
      eventsPublishedRate.mark(eventMessages.length)
      val futureMsg = kafkaPublisherTimer.time {
        Try(kafkaPublisher.beginTransaction()) match {
          case Failure(err) ⇒
            log.error(s"KafkaPublisherActor partition $assignedPartition there was an error beginning transaction $err")
            Future.successful(EventsFailedToPublish)
          case _ ⇒
            Future.sequence(kafkaPublisher.putRecords(records)).map { recordMeta ⇒
              log.info(s"KafkaPublisherActor partition {} committing transaction", assignedPartition)
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
                EventsFailedToPublish
            }
        }
      }
      context.become(processing(state.flushWrites().copy(transactionInProgress = true)))
      futureMsg.pipeTo(self)(sender())
    }
  }

  private def handle(state: KafkaProducerActorState, isAggregateStateCurrent: IsAggregateStateCurrent): Unit = {
    val aggregateId = isAggregateStateCurrent.aggregateId
    val noRecordsInFlight = state.inFlightForAggregate(aggregateId).isEmpty

    if (noRecordsInFlight) {
      rates.current.mark()
      sender() ! noRecordsInFlight
    } else {
      context.become(processing(state.addPendingInitialization(sender(), isAggregateStateCurrent)))
    }
  }

  private def getHealthCheck() = {
    val healthCheck = HealthCheck(
      name = "producer-actor",
      id = assignedTopicPartitionKey,
      status = HealthCheckStatus.UP)
    sender() ! healthCheck
  }

  private def getHealthCheck(state: KafkaProducerActorState) = {
    val healthCheck = HealthCheck(
      name = "producer-actor",
      id = assignedTopicPartitionKey,
      status = HealthCheckStatus.UP,
      details = Some(Map(
        "inFlight" -> state.inFlight.size.toString,
        "pendingInitializations" -> state.pendingInitializations.size.toString,
        "pendingWrites" -> state.pendingWrites.size.toString,
        "transactionInProgress" -> state.transactionInProgress.toString)))
    sender() ! healthCheck
  }
}

private[core] object KafkaProducerActorState {
  def empty(implicit sender: ActorRef, rates: KafkaProducerActorImpl.AggregateStateRates): KafkaProducerActorState = {
    KafkaProducerActorState(Seq.empty, Seq.empty, Seq.empty, transactionInProgress = false, sender = sender, rates = rates)
  }
}
// TODO optimize:
//  Add in a warning if state gets too large
private[core] case class KafkaProducerActorState(
    inFlight: Seq[KafkaRecordMetadata[String]],
    pendingWrites: Seq[KafkaProducerActorImpl.PublishWithSender],
    pendingInitializations: Seq[KafkaProducerActorImpl.PendingInitialization],
    transactionInProgress: Boolean,
    sender: ActorRef, rates: KafkaProducerActorImpl.AggregateStateRates) {

  import KafkaProducerActorImpl._

  private implicit val senderActor: ActorRef = sender
  private val log: Logger = LoggerFactory.getLogger(getClass)

  def inFlightByKey: Map[String, Seq[KafkaRecordMetadata[String]]] = {
    inFlight.groupBy(_.key.getOrElse(""))
  }

  def inFlightForAggregate(aggregateId: String): Seq[KafkaRecordMetadata[String]] = {
    inFlightByKey.getOrElse(aggregateId, Seq.empty)
  }

  def addPendingInitialization(sender: ActorRef, isAggregateStateCurrent: IsAggregateStateCurrent): KafkaProducerActorState = {
    val pendingInitialization = PendingInitialization(
      sender,
      isAggregateStateCurrent.aggregateId, isAggregateStateCurrent.expirationTime)

    this.copy(pendingInitializations = pendingInitializations :+ pendingInitialization)
  }

  def addPendingWrites(sender: ActorRef, publish: Publish): KafkaProducerActorState = {
    val newWriteRequest = PublishWithSender(sender, publish)
    this.copy(pendingWrites = pendingWrites :+ newWriteRequest)
  }

  def flushWrites(): KafkaProducerActorState = {
    this.copy(pendingWrites = Seq.empty)
  }

  def addInFlight(recordMetadata: Seq[KafkaRecordMetadata[String]]): KafkaProducerActorState = {
    val newTotalInFlight = inFlight ++ recordMetadata
    val newInFlight = newTotalInFlight.groupBy(_.key).mapValues(_.maxBy(_.wrapped.offset())).values

    this.copy(inFlight = newInFlight.toSeq)
  }

  def processedUpTo(stateMeta: KafkaPartitionMetadata): KafkaProducerActorState = {
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
        rates.current.mark(processedAggregates.length)
      }

      val expiredAggregates = pendingInitializations
        .filter(pending ⇒ Instant.now().isAfter(pending.expiration))
        .filterNot(processedAggregates.contains)

      if (expiredAggregates.nonEmpty) {
        expiredAggregates.foreach { pending ⇒
          log.debug(s"Aggregate ${pending.key} expiring since it is past ${pending.expiration}")
          pending.actor ! false
        }
        rates.notCurrent.mark(expiredAggregates.length)
      }
      pendingInitializations.filterNot(agg ⇒ processedAggregates.contains(agg) || expiredAggregates.contains(agg))
    }

    copy(inFlight = newInFlight, pendingInitializations = newPendingAggregates)
  }
}

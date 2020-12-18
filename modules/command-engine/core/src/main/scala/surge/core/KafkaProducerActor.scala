// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{ Actor, ActorRef, ActorSystem, NoSerializationVerificationNeeded, PoisonPill, Props, Stash, Status }
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{ ProducerConfig, ProducerRecord }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.header.{ Header, Headers }
import org.slf4j.{ Logger, LoggerFactory }
import surge.config.TimeoutConfig
import surge.kafka.streams.HealthyActor.GetHealth
import surge.kafka.streams.KafkaPartitionMetadataHandlerImpl.KafkaPartitionMetadataUpdated
import surge.kafka.streams._
import surge.metrics.{ MetricsProvider, Rate, Timer }
import surge.scala.core.kafka.{ KafkaBytesProducer, KafkaRecordMetadata }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

/**
 * A stateful producer actor responsible for publishing all states + events for
 * aggregates that belong to a particular state topic partition.  The state maintained
 * by this producer actor is a list of aggregate ids which are considered "in flight".
 * "In flight" is determined by keeping track of the offset this actor publishes to for
 * each aggregate id as messages are published to Kafka and listening to updates of the downstream
 * Kafka Streams consumer as it makes progress through the topic.  As a state is published,
 * this actor remembers the aggregate id and offset the state for that aggregate is.  When the
 * Kafka Streams consumer processes the state (saving it to a KTable) it notifies the MetadataHandler with
 * the offset of the most recently processed message. The MetadataHandler will publish an event KafkaPartitionMetadata
 * This producer actor subscribes to the KafkaPartitionMetadata events to get the last processed offset
 * and marks any aggregates in the "in flight" state as up to date
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
 * @param businessLogic Command service business logic wrapper used for determining state and event topics
 * @tparam Agg Generic aggregate type of aggregates publishing states/events through this stateful producer
 * @tparam Event Generic base type for events that aggregate instances publish through this stateful producer
 */
class KafkaProducerActor[Agg, Event](
    actorSystem: ActorSystem,
    assignedPartition: TopicPartition,
    metricsProvider: MetricsProvider,
    businessLogic: SurgeCommandBusinessLogic[Agg, _, Event]) extends HealthyComponent {

  private val log = LoggerFactory.getLogger(getClass)
  private val aggregateName: String = businessLogic.aggregateName

  private val publisherActor = actorSystem.actorOf(
    Props(new KafkaProducerActorImpl(
      assignedPartition, metricsProvider, businessLogic)).withDispatcher("kafka-publisher-actor-dispatcher"))

  def publish(aggregateId: String, state: KafkaProducerActor.MessageToPublish,
    events: Seq[KafkaProducerActor.MessageToPublish]): Future[KafkaProducerActor.PublishResult] = {
    log.trace(s"Publishing state for {} {}", Seq(aggregateName, state.key): _*)
    implicit val ec: ExecutionContext = ExecutionContext.global
    implicit val askTimeout: Timeout = Timeout(TimeoutConfig.PublisherActor.publishTimeout)
    (publisherActor ? KafkaProducerActorImpl.Publish(eventsToPublish = events, state = state))
      .mapTo[KafkaProducerActor.PublishResult]
  }

  def terminate(): Unit = {
    publisherActor ! PoisonPill
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
    publisherActor.ask(HealthyActor.GetHealth)(TimeoutConfig.HealthCheck.actorAskTimeout)
      .mapTo[HealthCheck]
      .recoverWith {
        case err: Throwable ⇒
          log.error(s"Failed to get publisher-actor health check", err)
          Future.successful(HealthCheck(
            name = "publisher-actor",
            id = aggregateName,
            status = HealthCheckStatus.DOWN))
      }(ExecutionContext.global)
  }
}

object KafkaProducerActor {
  sealed trait PublishResult
  case object PublishSuccess extends PublishResult
  case class PublishFailure(t: Throwable) extends PublishResult
  case class MessageToPublish(key: String, value: Array[Byte], headers: Headers)
}

private object KafkaProducerActorImpl {
  sealed trait KafkaProducerActorMessage extends NoSerializationVerificationNeeded
  case class Publish(state: KafkaProducerActor.MessageToPublish, eventsToPublish: Seq[KafkaProducerActor.MessageToPublish]) extends KafkaProducerActorMessage
  case class StateProcessed(stateMeta: KafkaPartitionMetadata) extends KafkaProducerActorMessage
  case class IsAggregateStateCurrent(aggregateId: String, expirationTime: Instant) extends KafkaProducerActorMessage
  case class AggregateStateRates(current: Rate, notCurrent: Rate)

  sealed trait InternalMessage extends NoSerializationVerificationNeeded
  case class EventsPublished(originalSenders: Seq[ActorRef], recordMetadata: Seq[KafkaRecordMetadata[String]]) extends InternalMessage
  case class EventsFailedToPublish(originalSenders: Seq[ActorRef], reason: Throwable) extends InternalMessage
  case class AbortTransactionFailed(originalSenders: Seq[ActorRef], abortTransactionException: Throwable, originalException: Throwable) extends InternalMessage
  case object InitTransactions extends InternalMessage
  case object FailedToInitTransactions extends InternalMessage
  case class Initialize(endOffset: Long) extends InternalMessage
  case object FailedToInitialize extends InternalMessage
  case object FlushMessages extends InternalMessage
  case class PublishWithSender(sender: ActorRef, publish: Publish) extends InternalMessage
  case class PendingInitialization(actor: ActorRef, key: String, expiration: Instant) extends InternalMessage
}
private class KafkaProducerActorImpl[Agg, Event](
    assignedPartition: TopicPartition, metrics: MetricsProvider,
    businessLogic: SurgeCommandBusinessLogic[Agg, _, Event],
    kafkaProducerOverride: Option[KafkaBytesProducer] = None) extends Actor with Stash {

  import KafkaProducerActorImpl._
  import businessLogic._
  import context.dispatcher
  import kafka._

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private val config = ConfigFactory.load()
  private val assignedTopicPartitionKey = s"${assignedPartition.topic}:${assignedPartition.partition}"
  private val flushInterval = config.getDuration("kafka.publisher.flush-interval", TimeUnit.MILLISECONDS).milliseconds
  private val publisherBatchSize = config.getInt("kafka.publisher.batch-size")
  private val publisherLingerMs = config.getInt("kafka.publisher.linger-ms")
  private val publisherCompression = config.getString("kafka.publisher.compression-type")
  private val publisherAcks = config.getString("kafka.publisher.acks")
  private val publisherTransactionTimeoutMs = config.getString("kafka.publisher.transaction-timeout-ms")
  private val brokers = config.getString("kafka.brokers").split(",")

  private val transactionalIdPrefix = businessLogic.transactionalIdPrefix
  // TODO revisit this - seems like a long transactional id, but may be close to what we want
  private val transactionalId = s"$transactionalIdPrefix-${assignedPartition.topic()}-${assignedPartition.partition()}"

  private var kafkaPublisher = getPublisher()

  private val nonTransactionalStatePublisher = kafkaProducerOverride.getOrElse(KafkaBytesProducer(brokers, stateTopic, partitioner = partitioner))

  private val kafkaPublisherTimer: Timer = metrics.createTimer(s"${aggregateName}KafkaWriteTimer")
  private implicit val rates: AggregateStateRates = AggregateStateRates(
    current = metrics.createRate(s"${aggregateName}AggregateStateCurrentRate"),
    notCurrent = metrics.createRate(s"${aggregateName}AggregateStateNotCurrentRate"))

  context.system.eventStream.subscribe(self, classOf[KafkaPartitionMetadataUpdated])

  context.system.scheduler.scheduleOnce(10.milliseconds, self, InitTransactions)
  context.system.scheduler.scheduleWithFixedDelay(flushInterval, flushInterval, self, FlushMessages)

  private def getPublisher(): KafkaBytesProducer = {
    kafkaProducerOverride.getOrElse(newPublisher())
  }

  private def newPublisher(): KafkaBytesProducer = {
    val kafkaConfig = Map[String, String](
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true.toString,
      ProducerConfig.BATCH_SIZE_CONFIG -> publisherBatchSize.toString,
      ProducerConfig.LINGER_MS_CONFIG -> publisherLingerMs.toString,
      ProducerConfig.COMPRESSION_TYPE_CONFIG -> publisherCompression,
      ProducerConfig.ACKS_CONFIG -> publisherAcks,
      ProducerConfig.TRANSACTION_TIMEOUT_CONFIG -> publisherTransactionTimeoutMs.toString,
      ProducerConfig.TRANSACTIONAL_ID_CONFIG → transactionalId)

    // TODO Expose Kafka Producer Metrics
    // Set up the producer on the events topic so the partitioner can partition automatically on the events topic since we manually set the partition for the
    // aggregate state topic record and the events topic could have a different number of partitions
    KafkaBytesProducer(brokers, eventsTopic, partitioner, kafkaConfig)
  }

  override def receive: Receive = uninitialized

  // scalastyle:off cyclomatic.complexity
  private def uninitialized: Receive = {
    case InitTransactions                 ⇒ initializeTransactions()
    case msg: Initialize                  ⇒ handle(msg)
    case FailedToInitialize               ⇒ context.system.scheduler.scheduleOnce(3.seconds)(initializeState())
    case FlushMessages                    ⇒ // Ignore from this state
    case GetHealth                        ⇒ getHealthCheck()
    case _: KafkaPartitionMetadataUpdated ⇒ stash() // process only AFTER flush message is sent
    case _: Publish                       ⇒ stash()
    case _: StateProcessed                ⇒ stash()
    case _: IsAggregateStateCurrent       ⇒ stash()
    case unknown                          ⇒ log.warn("Receiving unhandled message {} on uninitialized state", unknown.getClass.getName)
  }
  // scalastyle:on

  private def recoveringBacklog(endOffset: Long): Receive = {
    case msg: KafkaPartitionMetadataUpdated ⇒ handle(msg)
    case msg: StateProcessed                ⇒ handleFromRecoveringState(endOffset, msg)
    case FlushMessages                      ⇒ // Ignore from this state
    case GetHealth                          ⇒ getHealthCheck()
    case _: Publish                         ⇒ stash()
    case _: IsAggregateStateCurrent         ⇒ stash()
    case unknown                            ⇒ log.warn("Receiving unhandled message {} on recoveringBacklog state", unknown.getClass.getName)
  }

  private def processing(state: KafkaProducerActorState): Receive = {
    case msg: KafkaPartitionMetadataUpdated ⇒ handle(msg)
    case msg: Publish                       ⇒ handle(state, msg)
    case msg: EventsPublished               ⇒ handle(state, msg)
    case msg: EventsFailedToPublish         ⇒ handleFailedToPublish(state, msg)
    case msg: StateProcessed                ⇒ handle(state, msg)
    case msg: IsAggregateStateCurrent       ⇒ handle(state, msg)
    case GetHealth                          ⇒ getHealthCheck(state)
    case FlushMessages                      ⇒ handleFlushMessages(state)
    case Done                               ⇒ // Ignore to prevent these messages to become dead letters
    case msg: AbortTransactionFailed        ⇒ handle(msg)
    case Status.Failure(e) ⇒
      log.error(s"Saw unhandled exception in producer for $assignedPartition", e)
  }

  private def sendFlushRecord: Future[InternalMessage] = {
    val flushRecord = new ProducerRecord[String, Array[Byte]](assignedPartition.topic(), assignedPartition.partition(), "", "".getBytes)

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
        log.error(s"KafkaPublisherActor failed to initialize kafka transactions, retrying in 3 seconds: $assignedPartition", err)
        closeAndRecreatePublisher()
        context.system.scheduler.scheduleOnce(3.seconds, self, InitTransactions)
    }
  }

  private def closeAndRecreatePublisher(): Unit = {
    Try(kafkaPublisher.close())
    kafkaPublisher = getPublisher()
  }

  private def initializeState(): Unit = {
    sendFlushRecord pipeTo self
  }

  private def handle(event: KafkaPartitionMetadataUpdated): Unit = {
    if (event.value.topicPartition == assignedTopicPartitionKey) {
      self ! StateProcessed(event.value)
    }
  }

  private def handle(initialize: Initialize): Unit = {
    log.info(s"Publisher actor initializing for topic-partition $assignedPartition with end offset ${initialize.endOffset}")
    context.become(recoveringBacklog(initialize.endOffset))
    unstashAll()
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
    val newState = state.addInFlight(eventsPublished.recordMetadata).completeTransaction()
    context.become(processing(newState))
    eventsPublished.originalSenders.foreach(_ ! KafkaProducerActor.PublishSuccess)
  }

  private def handleFailedToPublish(state: KafkaProducerActorState, msg: EventsFailedToPublish): Unit = {
    val newState = state.completeTransaction()
    context.become(processing(newState))
    msg.originalSenders.foreach(_ ! KafkaProducerActor.PublishFailure(msg.reason))
  }

  private var lastTransactionInProgressWarningTime: Instant = Instant.ofEpochMilli(0L)
  private val transactionTimeWarningThreshold = flushInterval.toMillis * 4
  private val eventsPublishedRate: Rate = metrics.createRate(s"${aggregateName}EventPublishRate")
  private def handleFlushMessages(state: KafkaProducerActorState): Unit = {
    if (state.transactionInProgress) {
      if (state.currentTransactionTimeMillis >= transactionTimeWarningThreshold &&
        lastTransactionInProgressWarningTime.plusSeconds(1L).isBefore(Instant.now())) {

        lastTransactionInProgressWarningTime = Instant.now
        log.warn(s"KafkaPublisherActor partition {} tried to flush, but another transaction is already in progress. " +
          s"The previous transaction has been in progress for {} milliseconds. If the time to complete the previous transaction continues to grow " +
          s"that typically indicates slowness in the Kafka brokers.", assignedPartition, state.currentTransactionTimeMillis)
      }
    } else if (state.pendingWrites.nonEmpty) {
      val eventMessages = state.pendingWrites.flatMap(_.publish.eventsToPublish)
      val stateMessages = state.pendingWrites.map(_.publish.state)

      val eventRecords = eventMessages.map { eventToPublish ⇒
        // Using null here since we need to add the headers but we don't want to explicitly assign the partition
        new ProducerRecord(eventsTopic.name, null, eventToPublish.key, eventToPublish.value, eventToPublish.headers) // scalastyle:ignore null
      }
      val stateRecords = stateMessages.map { state ⇒
        new ProducerRecord(stateTopic.name, assignedPartition.partition(), state.key, state.value, state.headers)
      }
      val records = eventRecords ++ stateRecords

      log.debug(s"KafkaPublisherActor partition {} writing {} events to Kafka", assignedPartition, eventRecords.length)
      log.debug(s"KafkaPublisherActor partition {} writing {} states to Kafka", assignedPartition, stateRecords.length)
      eventsPublishedRate.mark(eventMessages.length)
      doFlushRecords(state, records)
    }
  }

  private def doFlushRecords(state: KafkaProducerActorState, records: Seq[ProducerRecord[String, Array[Byte]]]): Unit = {
    val senders = state.pendingWrites.map(_.sender)
    val futureMsg = kafkaPublisherTimer.time {
      Try(kafkaPublisher.beginTransaction()) match {
        case Failure(f: ProducerFencedException) ⇒
          producerFenced()
          Future.successful(EventsFailedToPublish(senders, f)) // Only used for the return type, the actor is stopped in the producerFenced() method
        case Failure(err) ⇒
          log.error(s"KafkaPublisherActor partition $assignedPartition there was an error beginning transaction", err)
          Future.successful(EventsFailedToPublish(senders, err))
        case _ ⇒
          Future.sequence(kafkaPublisher.putRecords(records)).map { recordMeta ⇒
            log.debug(s"KafkaPublisherActor partition {} committing transaction", assignedPartition)
            kafkaPublisher.commitTransaction()
            EventsPublished(senders, recordMeta.filter(_.wrapped.topic() == stateTopic.name))
          } recover {
            case _: ProducerFencedException ⇒
              producerFenced()
            case e ⇒
              log.error(s"KafkaPublisherActor partition $assignedPartition got error while trying to publish to Kafka", e)
              Try(kafkaPublisher.abortTransaction()) match {
                case Success(_) ⇒
                  EventsFailedToPublish(senders, e)
                case Failure(exception) ⇒
                  AbortTransactionFailed(senders, abortTransactionException = exception, originalException = e)
              }
          }
      }
    }
    context.become(processing(state.flushWrites().startTransaction()))
    futureMsg.pipeTo(self)(sender())
  }

  private def handle(abortTransactionFailed: AbortTransactionFailed): Unit = {
    log.error(
      s"KafkaPublisherActor partition $assignedPartition saw an error aborting transaction, will recreate the producer.",
      abortTransactionFailed.abortTransactionException)
    abortTransactionFailed.originalSenders.foreach(_ ! KafkaProducerActor.PublishFailure(abortTransactionFailed.originalException))
    closeAndRecreatePublisher()
    context.system.scheduler.scheduleOnce(10.milliseconds, self, InitTransactions)
    context.become(uninitialized)
  }

  private def producerFenced(): Unit = {
    val producerFencedErrorLog = s"KafkaPublisherActor partition $assignedPartition tried to commit a transaction, but was " +
      s"fenced out by another producer instance. This instance of the producer for the assigned partition will shut down in favor of the " +
      s"newer producer for this partition.  If this message persists, check that two independent application clusters are not using the same " +
      s"transactional id prefix of [$transactionalId] for the same Kafka cluster."
    log.error(producerFencedErrorLog)
    context.stop(self)
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

  private def getHealthCheck(): Unit = {
    val healthCheck = HealthCheck(
      name = "producer-actor",
      id = assignedTopicPartitionKey,
      status = HealthCheckStatus.UP)
    sender() ! healthCheck
  }

  private def getHealthCheck(state: KafkaProducerActorState): Unit = {
    val transactionsAppearStuck = state.currentTransactionTimeMillis > 2.minutes.toMillis

    val healthStatus = if (transactionsAppearStuck) {
      HealthCheckStatus.DOWN
    } else {
      HealthCheckStatus.UP
    }

    val healthCheck = HealthCheck(
      name = "producer-actor",
      id = assignedTopicPartitionKey,
      status = healthStatus,
      details = Some(Map(
        "inFlight" → state.inFlight.size.toString,
        "pendingInitializations" → state.pendingInitializations.size.toString,
        "pendingWrites" → state.pendingWrites.size.toString,
        "currentTransactionTimeMillis" → state.currentTransactionTimeMillis.toString)))
    sender() ! healthCheck
  }
}

private[core] object KafkaProducerActorState {
  def empty(implicit sender: ActorRef, rates: KafkaProducerActorImpl.AggregateStateRates): KafkaProducerActorState = {
    KafkaProducerActorState(Seq.empty, Seq.empty, Seq.empty, transactionInProgressSince = None, sender = sender, rates = rates)
  }
}
// TODO optimize:
//  Add in a warning if state gets too large
private[core] case class KafkaProducerActorState(
    inFlight: Seq[KafkaRecordMetadata[String]],
    pendingWrites: Seq[KafkaProducerActorImpl.PublishWithSender],
    pendingInitializations: Seq[KafkaProducerActorImpl.PendingInitialization],
    transactionInProgressSince: Option[Instant],
    sender: ActorRef, rates: KafkaProducerActorImpl.AggregateStateRates) {

  import KafkaProducerActorImpl._

  private implicit val senderActor: ActorRef = sender
  private val log: Logger = LoggerFactory.getLogger(getClass)

  def transactionInProgress: Boolean = transactionInProgressSince.nonEmpty
  def currentTransactionTimeMillis: Long = {
    Instant.now.minusMillis(transactionInProgressSince.getOrElse(Instant.now).toEpochMilli).toEpochMilli
  }

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

  def startTransaction(): KafkaProducerActorState = {
    this.copy(transactionInProgressSince = Some(Instant.now))
  }

  def completeTransaction(): KafkaProducerActorState = {
    this.copy(transactionInProgressSince = None)
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
      log.trace(s"${stateMeta.topic}:${stateMeta.partition} processed up to offset ${stateMeta.offset}. " +
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

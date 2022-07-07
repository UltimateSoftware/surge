// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import akka.actor.{ Actor, ActorRef, NoSerializationVerificationNeeded, Stash, Status, Timers }
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.kafka.clients.admin.ListOffsetsOptions
import org.apache.kafka.clients.producer.{ ProducerConfig, ProducerRecord }
import org.apache.kafka.common.errors.{ AuthorizationException, ProducerFencedException }
import org.apache.kafka.common.{ IsolationLevel, TopicPartition }
import org.slf4j.{ Logger, LoggerFactory }
import surge.core.KafkaProducerActor
import surge.core.KafkaProducerActor.{ PublishTracker, PublishTrackerWithExpiry }
import surge.health.{ HealthSignalBusTrait, HealthyPublisher }
import surge.internal.akka.cluster.ActorHostAwareness
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.config.TimeoutConfig
import surge.internal.health.{ HealthCheck, HealthCheckStatus }
import surge.kafka._
import surge.internal.health.HealthyActor.GetHealth
import surge.internal.kafka.KafkaProducerActorImpl.PublishTrackerStateManager
import surge.metrics.{ MetricInfo, Metrics, Rate, Timer }

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

object KafkaProducerActorImpl {
  val KAFKA_PRODUCER_KTABLE_ERROR_SIGNAL_NAME: String = "kafka.producer.actor.ktable.error"
  val KAFKA_PRODUCER_ABORT_TX_FAILED_SIGNAL_NAME: String = "kafka.producer.actor.abort.tx.failed"
  val KAFKA_PRODUCER_FENCED_SIGNAL_NAME: String = "kafka.producer.actor.fenced"

  sealed trait KafkaProducerActorMessage extends NoSerializationVerificationNeeded

  class PublishTrackerStateManager(val trackerTimeout: FiniteDuration = TimeoutConfig.PublisherActor.trackerTimeout) {
    def tracker(id: UUID, state: KafkaProducerActorState): Option[PublishTrackerWithExpiry] = {
      state.tracker(id)
    }

    def stateHasPendingWrites(id: UUID, state: KafkaProducerActorState): Boolean = {
      state.hasPendingWrites(id)
    }
  }

  case class Publish(batchId: UUID, state: KafkaProducerActor.MessageToPublish, eventsToPublish: Seq[KafkaProducerActor.MessageToPublish])
      extends KafkaProducerActorMessage {}

  case class IsAggregateStateCurrent(aggregateId: String) extends KafkaProducerActorMessage
  case object ShutdownProducer extends KafkaProducerActorMessage

  object AggregateStateRates {
    def apply(aggregateName: String, metrics: Metrics): AggregateStateRates = AggregateStateRates(
      current = metrics.rate(
        MetricInfo(
          name = s"surge.aggregate.state-current-rate",
          description = "The per-second rate of aggregates that are up-to-date in and can be loaded immediately from the KTable",
          tags = Map("aggregate" -> aggregateName))),
      notCurrent = metrics.rate(
        MetricInfo(
          name = s"surge.aggregate.state-not-current-rate",
          description = "The per-second rate of aggregates that are not up-to-date in the KTable and must wait to be loaded",
          tags = Map("aggregate" -> aggregateName))))

  }

  case class SenderWithTrackingId(actorRef: ActorRef, trackingId: UUID)
  case class AggregateStateRates(current: Rate, notCurrent: Rate)

  sealed trait InternalMessage extends NoSerializationVerificationNeeded
  case class EventsPublished(originalSenders: Seq[SenderWithTrackingId], recordMetadata: Seq[KafkaRecordMetadata[String]] = Seq.empty) extends InternalMessage
  case class EventsFailedToPublish(originalSenders: Seq[SenderWithTrackingId], reason: Throwable) extends InternalMessage
  case class AbortTransactionFailed(originalSenders: Seq[SenderWithTrackingId], abortTransactionException: Throwable, originalException: Throwable)
      extends InternalMessage
  case object InitTransactions extends InternalMessage
  case object ClearExpiredTrackers extends InternalMessage
  case object InitTransactionSuccess extends InternalMessage
  case object FailedToInitTransactions extends InternalMessage
  case object FlushMessages extends InternalMessage
  case object CheckKTableProgress extends InternalMessage
  case class PublishWithSender(senderWithTrackingId: SenderWithTrackingId, publish: Publish) extends InternalMessage {
    def sender(): ActorRef = senderWithTrackingId.actorRef
  }

  case class PendingInitialization(actor: ActorRef, key: String, expiration: Instant) extends InternalMessage
  case class KTableProgressUpdate(topicPartition: TopicPartition, lagInfo: LagInfo) extends InternalMessage
  case class ProducerFenced(exception: ProducerFencedException) extends InternalMessage
  case object RestartProducer extends InternalMessage
}

class KafkaProducerActorImpl(
    assignedPartition: TopicPartition,
    metrics: Metrics,
    producerContext: ProducerActorContext,
    lagChecker: KTableLagChecker,
    partitionTracker: KafkaConsumerPartitionAssignmentTracker,
    override val signalBus: HealthSignalBusTrait,
    config: Config,
    publishTrackerStateManager: PublishTrackerStateManager = new PublishTrackerStateManager(TimeoutConfig.PublisherActor.trackerTimeout),
    kafkaProducerOverride: Option[KafkaProducerTrait[String, Array[Byte]]] = None)
    extends Actor
    with ActorHostAwareness
    with Stash
    with Timers
    with HealthyPublisher {

  import KafkaProducerActorImpl._
  import context.dispatcher
  import producerContext._
  import kafka._

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private val assignedTopicPartitionKey = s"${assignedPartition.topic}:${assignedPartition.partition}"
  private val flushInterval = config.getDuration("kafka.publisher.flush-interval", TimeUnit.MILLISECONDS).milliseconds
  private val transactionTimeWarningThresholdMillis = config.getDuration("kafka.publisher.transaction-warning-time", TimeUnit.MILLISECONDS)
  private val publisherTransactionTimeoutMs = config.getString("kafka.publisher.transaction-timeout-ms")
  private val ktableCheckInterval = config.getDuration("kafka.publisher.ktable-check-interval").toMillis.milliseconds
  private val brokers = config.getString("kafka.brokers").split(",").toVector
  private val enableMetrics = config.getBoolean("surge.producer.enable-kafka-metrics")
  private val initTransactionAuthzExceptionRetryDuration =
    config.getDuration("kafka.publisher.init-transactions.authz-exception-retry-time").toMillis.millis.toCoarsest
  private val initTransactionOtherExceptionRetryDuration =
    config.getDuration("kafka.publisher.init-transactions.other-exception-retry-time").toMillis.millis.toCoarsest
  private val producerFencedStateUnhealthyReportTime = config.getDuration("kafka.publisher.fenced-unhealthy-report-time").toMillis.milliseconds

  private val disableTransactionsExperimental = config.getBoolean("surge.feature-flags.experimental.disable-single-record-transactions")

  private val transactionalId = s"$transactionalIdPrefix-${assignedPartition.topic()}-${assignedPartition.partition()}"
  private val kafkaPublisherMetricsName = transactionalId

  // noinspection ActorMutableStateInspection
  private var kafkaPublisher = getPublisher

  private val nonTransactionalStatePublisher =
    kafkaProducerOverride.getOrElse(KafkaProducer.bytesProducer(config, brokers, stateTopic, partitioner = partitioner))

  private val kafkaPublisherTimer: Timer = metrics.timer(
    MetricInfo(
      s"surge.aggregate.kafka-write-timer",
      "Average time in milliseconds that it takes the publisher to write a batch of messages (events & state) to Kafka",
      tags = Map("aggregate" -> aggregateName)))
  private implicit val rates: AggregateStateRates = AggregateStateRates(aggregateName, metrics)

  context.system.scheduler.scheduleOnce(10.milliseconds, self, InitTransactions)

  private val clearExpiredTrackersScheduledTask = context.system.scheduler.scheduleAtFixedRate(10.milliseconds, 10.seconds, self, ClearExpiredTrackers)

  private val flushMessagesScheduledTask = context.system.scheduler.scheduleWithFixedDelay(flushInterval, flushInterval, self, FlushMessages)
  private val checkKTableLagScheduledTask = context.system.scheduler.scheduleAtFixedRate(ktableCheckInterval, ktableCheckInterval, self, CheckKTableProgress)

  override def postStop(): Unit = {
    flushMessagesScheduledTask.cancel()
    checkKTableLagScheduledTask.cancel()
    clearExpiredTrackersScheduledTask.cancel()
    super.postStop()
  }

  private def getPublisher: KafkaProducerTrait[String, Array[Byte]] = {
    kafkaProducerOverride.getOrElse(newPublisher())
  }

  private def newPublisher(): KafkaProducerTrait[String, Array[Byte]] = {

    object PoisonTopic extends KafkaTopicTrait {
      def name = throw new IllegalStateException("there is no topic")
    }

    // Because we use transactions, we must force enable idempotence to true and acks=all
    val kafkaConfig = Map[String, String](
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true.toString,
      ProducerConfig.ACKS_CONFIG -> "all",
      ProducerConfig.TRANSACTION_TIMEOUT_CONFIG -> publisherTransactionTimeoutMs,
      ProducerConfig.TRANSACTIONAL_ID_CONFIG -> transactionalId)

    // Set up the producer on the events topic so the partitioner can partition automatically on the events topic since we manually set the partition for the
    // aggregate state topic record and the events topic could have a different number of partitions
    val producer = KafkaProducer.bytesProducer(config, brokers, eventsTopicOpt.getOrElse(PoisonTopic), partitioner, kafkaConfig)
    if (enableMetrics) {
      metrics.registerKafkaMetrics(kafkaPublisherMetricsName, () => producer.producer.metrics)
    }
    producer
  }

  override def receive: Receive = uninitialized(None)

  override def signalMetadata(): Map[String, String] = {
    Map[String, String](elems = "aggregateName" -> aggregateName)
  }

  private def uninitialized(lastProgressUpdate: Option[KTableProgressUpdate]): Receive = {
    case CheckKTableProgress    => checkKTableProgress()
    case InitTransactions       => initializeTransactions()
    case InitTransactionSuccess => initTransactionsSuccess(lastProgressUpdate)
    case FlushMessages          => log.trace("KafkaProducerActor ignoring FlushMessages message from the uninitialized state")
    case failedToPublish: EventsFailedToPublish =>
      failedToPublish.originalSenders.foreach(sender => sender.actorRef ! KafkaProducerActor.PublishFailure(sender.trackingId, failedToPublish.reason))
    case GetHealth =>
      sender() ! HealthCheck(
        name = "producer-actor",
        id = assignedTopicPartitionKey,
        status = HealthCheckStatus.UP,
        details = Some(Map("state" -> "uninitialized")))
    case update: KTableProgressUpdate => context.become(uninitialized(lastProgressUpdate = Some(update)))
    case ShutdownProducer             => stopPublisher()
    case _: Publish                   => stash()
    case _: IsAggregateStateCurrent   => sender().tell(false, self)
    case ClearExpiredTrackers         => // Ignore and do nothing
  }

  private def waitingForKTableIndexing(): Receive = {
    case CheckKTableProgress       => checkKTableProgress()
    case msg: KTableProgressUpdate => handleFromWaitingForKTableIndexingState(msg)
    case FlushMessages             => log.trace("KafkaProducerActor ignoring FlushMessages message from the waitingForKTableIndexing state")
    case ShutdownProducer          => stopPublisher()
    case failedToPublish: EventsFailedToPublish =>
      failedToPublish.originalSenders.foreach(sender => sender.actorRef ! KafkaProducerActor.PublishFailure(sender.trackingId, failedToPublish.reason))
    case GetHealth =>
      sender() ! HealthCheck(
        name = "producer-actor",
        id = assignedTopicPartitionKey,
        status = HealthCheckStatus.UP,
        details = Some(Map("state" -> "waitingForKTableIndexing")))
    case ClearExpiredTrackers       => // Ignore and do nothing
    case _: Publish                 => stash()
    case _: IsAggregateStateCurrent => sender().tell(false, self)
  }

  private def fenced(state: KafkaProducerActorState, initialFenceTime: Instant): Receive = {
    case msg: ProducerFenced => maybeRestartFencedProducer(msg)
    case msg: EventsFailedToPublish =>
      context.become(fenced(state.completeTransaction(), initialFenceTime))
      msg.originalSenders.foreach(sender => sender.actorRef ! KafkaProducerActor.PublishFailure(sender.trackingId, msg.reason))
    case RestartProducer  => restartPublisher()
    case ShutdownProducer => stopPublisher()
    case ClearExpiredTrackers =>
      context.become(fenced(state.clearExpiredTrackers(), initialFenceTime))
    case CheckKTableProgress          => log.trace("KafkaProducerActor ignoring CheckKTableProgress message from the fenced state")
    case FlushMessages                => log.trace("KafkaProducerActor ignoring FlushMessages message from the fenced state")
    case update: KTableProgressUpdate => context.become(fenced(state.processedUpTo(update), initialFenceTime))
    case GetHealth                    => doHealthCheck(initialFenceTime)
    case _                            => stash()
  }

  private def processing(state: KafkaProducerActorState): Receive = {
    case msg: InternalMessage                                  => handleProcessingInternalMessage(msg, state)
    case msg: KafkaProducerActorImpl.KafkaProducerActorMessage => handleProcessingProducerMessage(msg, state)
    case GetHealth                                             => doHealthCheck(state)
//    case ClearExpiredTrackers =>
//      context.become(processing(state.clearExpiredTrackers()))
    case ShutdownProducer => stopPublisher()
    case Status.Failure(e) =>
      log.error(s"Saw unhandled exception in producer for $assignedPartition", e)
  }

  private def handleProcessingProducerMessage(message: KafkaProducerActorImpl.KafkaProducerActorMessage, state: KafkaProducerActorState): Unit = {
    message match {
      case msg: Publish =>
        val maybeTrackerWithExpiry = publishTrackerStateManager.tracker(msg.batchId, state)
        maybeTrackerWithExpiry match {
          case Some(trackerWithExpiry) =>
            if (!trackerWithExpiry.tracker.dataWasPublished) {
              if (!trackerWithExpiry.expired) {
                // Avoid duplicating a write that is already pending.
                if (!publishTrackerStateManager.stateHasPendingWrites(msg.batchId, state)) {
                  context.become(processing(state.addPendingWrites(sender(), msg)))
                } else {
                  context.become(processing(state.updateSenderForPendingWrite(sender(), msg)))
                }
              } else {
                context.become(processing(state.stopTracking(msg)))
                handleFailedToPublish(
                  state,
                  EventsFailedToPublish(
                    reason = new RuntimeException(s"PublishTracker expired, not attempting another publish for ${msg.batchId}"),
                    originalSenders = Seq(SenderWithTrackingId(sender(), msg.batchId))))
              }
            } else {
              context.become(processing(state))
              handle(state, EventsPublished(Seq(SenderWithTrackingId(sender(), msg.batchId))))
            }
          case None =>
            context.become(processing(state.addPendingWrites(sender(), msg).startTracking(msg, publishTrackerStateManager.trackerTimeout)))
        }

      case msg: IsAggregateStateCurrent => handle(state, msg)
      case other                        => unhandled(other)
    }
  }

  private def handleProcessingInternalMessage(message: InternalMessage, state: KafkaProducerActorState): Unit = {
    message match {
      case CheckKTableProgress       => checkKTableProgress()
      case msg: KTableProgressUpdate => context.become(processing(state.processedUpTo(msg)))
      case msg: EventsPublished      => handle(state, msg)
      case msg: EventsFailedToPublish =>
        handleFailedToPublish(state, msg)
      case KafkaProducerActorImpl.ClearExpiredTrackers => context.become(processing(state.clearExpiredTrackers()))
      case FlushMessages                               => handleFlushMessages(state)
      case msg: AbortTransactionFailed                 => handle(msg)
      case msg: ProducerFenced =>
        context.become(fenced(state, Instant.now))
        self ! msg
      case KafkaProducerActorImpl.InitTransactions         => unhandled()
      case KafkaProducerActorImpl.InitTransactionSuccess   => unhandled()
      case KafkaProducerActorImpl.FailedToInitTransactions => unhandled()
      case KafkaProducerActorImpl.RestartProducer          => unhandled()
      case msg: PublishWithSender                          => unhandled(msg)
      case msg: PendingInitialization                      => unhandled(msg)

      case msg: KTableProgressUpdate => unhandled(msg)

      case other => unhandled(other)
    }
  }

  private def checkKTableProgress(): Unit = {
    Future {
      lagChecker.getConsumerGroupLag(assignedPartition)
    }.onComplete {
      case Success(Some(lag)) =>
        self ! KTableProgressUpdate(assignedPartition, lag)
      case Success(None) =>
        log.debug(s"Could not find partition lag for partition $assignedPartition")
      case Failure(exception) =>
        log.debug(s"Could not find partition lag for partition $assignedPartition", exception)
    }
  }

  private def initializeTransactions(): Unit = {
    val flushRecord = new ProducerRecord[String, Array[Byte]](assignedPartition.topic(), assignedPartition.partition(), "", "".getBytes)
    kafkaPublisher
      .initTransactions()
      .flatMap { _ =>
        log.debug(s"KafkaProducerActor transactions successfully initialized: $assignedPartition")
        nonTransactionalStatePublisher.putRecord(flushRecord).map { _ =>
          self ! InitTransactionSuccess
        }
      }
      .recover { case err: Throwable =>
        val retryTime = initTransactionsRetryTimeFromError(err)
        log.error(
          s"KafkaProducerActor failed to initialize kafka transactions with transactional id [$transactionalId] " +
            s"for partition [$assignedPartition], retrying in $retryTime",
          err)
        closeAndRecreatePublisher()
        context.system.scheduler.scheduleOnce(retryTime, self, InitTransactions)
      }
  }

  private def initTransactionsSuccess(lastProgressUpdate: Option[KTableProgressUpdate]): Unit = {
    unstashAll()
    context.become(waitingForKTableIndexing())
    lastProgressUpdate.foreach(update => self ! update)
  }

  private def initTransactionsRetryTimeFromError(err: Throwable): FiniteDuration = {
    err match {
      case _: AuthorizationException => initTransactionAuthzExceptionRetryDuration
      case _                         => initTransactionOtherExceptionRetryDuration
    }
  }

  private def closePublisher(): Unit = {
    if (enableMetrics) {
      metrics.unregisterKafkaMetric(kafkaPublisherMetricsName)
    }
    Try(kafkaPublisher.close())
  }

  private def closeAndRecreatePublisher(): Unit = {
    closePublisher()
    kafkaPublisher = getPublisher
  }

  private def handleFromWaitingForKTableIndexingState(kTableProgressUpdate: KTableProgressUpdate): Unit = {
    val currentLag = kTableProgressUpdate.lagInfo.offsetLag
    if (currentLag == 0L) {
      log.info(s"KafkaProducerActor partition {} is fully up to date on processing", assignedPartition)
      unstashAll()
      context.become(processing(KafkaProducerActorState.empty))
    } else {
      log.debug("Producer actor {} still waiting for KTable to finish indexing, current lag is {}", assignedPartition, kTableProgressUpdate.lagInfo.offsetLag)
    }
  }

  private def handle(state: KafkaProducerActorState, eventsPublished: EventsPublished): Unit = {
    if (eventsPublished.recordMetadata.nonEmpty) {
      val newState = state.addInFlight(eventsPublished.recordMetadata).completeTransaction()
      context.become(processing(newState))
    }

    eventsPublished.originalSenders.foreach(sender => {
      sender.actorRef ! KafkaProducerActor.PublishSuccess(sender.trackingId)
    })
  }

  private def handleFailedToPublish(state: KafkaProducerActorState, msg: EventsFailedToPublish): Unit = {
    val newState = state.completeTransaction()
    context.become(processing(newState))
    msg.originalSenders.foreach(sender => sender.actorRef ! KafkaProducerActor.PublishFailure(sender.trackingId, msg.reason))
  }

  private val eventsPublishedRate: Rate = metrics.rate(
    MetricInfo(
      name = s"surge.aggregate.event-publish-rate",
      description = "The per-second rate at which this aggregate attempts to publish events to Kafka",
      tags = Map("aggregate" -> aggregateName)))
  private def handleFlushMessages(state: KafkaProducerActorState): Unit = {
    if (state.transactionInProgress) {
      if (state.currentTransactionTimeMillis >= transactionTimeWarningThresholdMillis &&
        state.lastTransactionInProgressWarningTime.plusMillis(transactionTimeWarningThresholdMillis).isBefore(Instant.now())) {
        val newState = state.copy(lastTransactionInProgressWarningTime = Instant.now)
        log.warn(
          s"KafkaProducerActor partition {} tried to flush, but another transaction is already in progress. " +
            s"The previous transaction has been in progress for {} milliseconds. If the time to complete the previous transaction continues to grow " +
            s"that typically indicates slowness in the Kafka brokers.",
          assignedPartition,
          state.currentTransactionTimeMillis)
        context.become(processing(newState))
      }
    } else if (state.pendingWrites.nonEmpty) {
      val senders = state.pendingWrites.map(_.senderWithTrackingId)
      val eventMessages = state.pendingWrites.flatMap(_.publish.eventsToPublish)
      val stateMessages = state.pendingWrites.map(_.publish.state)

      val eventRecords = eventsTopicOpt
        .map(eventsTopic =>
          eventMessages.map { eventToPublish =>
            // Using null here since we need to add the headers but we don't want to explicitly assign the partition
            new ProducerRecord(eventsTopic.name, null, eventToPublish.key, eventToPublish.value, eventToPublish.headers) // scalastyle:ignore null
          })
        .getOrElse(Seq.empty)

      val stateRecords = stateMessages.map { state =>
        new ProducerRecord(stateTopic.name, assignedPartition.partition(), state.key, state.value, state.headers)
      }
      val records = eventRecords ++ stateRecords

      log.debug(s"KafkaProducerActor partition {} writing {} events to Kafka", assignedPartition, eventRecords.length)
      log.debug(s"KafkaProducerActor partition {} writing {} states to Kafka", assignedPartition, stateRecords.length)
      eventsPublishedRate.mark(eventMessages.length)

      doFlushRecords(state, senders, records)
    }
  }

  private def publishRecordsWithTransaction(senders: Seq[SenderWithTrackingId], records: Seq[ProducerRecord[String, Array[Byte]]]): Future[InternalMessage] = {
    kafkaPublisherTimer.timeFuture {
      Try(kafkaPublisher.beginTransaction()) match {
        case Failure(f: ProducerFencedException) =>
          producerFenced(f)
          Future.successful(EventsFailedToPublish(senders, f))
        case Failure(err) =>
          log.error(s"KafkaProducerActor partition $assignedPartition there was an error beginning transaction", err)
          Future.successful(EventsFailedToPublish(senders, err))
        case _ =>
          Future
            .sequence(kafkaPublisher.putRecords(records))
            .map { recordMeta =>
              log.debug(s"KafkaProducerActor partition {} committing transaction", assignedPartition)
              kafkaPublisher.commitTransaction()
              EventsPublished(senders, recordMeta.filter(_.wrapped.topic() == stateTopic.name))
            }
            .recover {
              case e: ProducerFencedException =>
                producerFenced(e)
                EventsFailedToPublish(senders, e)
              case e =>
                log.error(s"KafkaProducerActor partition $assignedPartition got error while trying to publish to Kafka", e)
                Try(kafkaPublisher.abortTransaction()) match {
                  case Success(_) =>
                    EventsFailedToPublish(senders, e)
                  case Failure(exception) =>
                    AbortTransactionFailed(originalSenders = senders, abortTransactionException = exception, originalException = e)
                }
            }
      }
    }
  }

  private def publishSingleRecord(senders: Seq[SenderWithTrackingId], record: ProducerRecord[String, Array[Byte]]): Future[InternalMessage] = {
    kafkaPublisherTimer.timeFuture {
      nonTransactionalStatePublisher
        .putRecord(record)
        .map { rm =>
          log.debug(s"KafkaProducerActor partition {} wrote single message without a transaction", assignedPartition)
          EventsPublished(senders, Seq(rm))
        }
        .recover { case e =>
          log.error(s"KafkaProducerActor partition $assignedPartition got error while trying to publish to Kafka", e)
          EventsFailedToPublish(senders, e)
        }
    }
  }

  private def doFlushRecords(state: KafkaProducerActorState, senders: Seq[SenderWithTrackingId], records: Seq[ProducerRecord[String, Array[Byte]]]): Unit = {
    val futureMsg = if (records.size > 1 || !disableTransactionsExperimental) {
      val fut = publishRecordsWithTransaction(senders, records)
      context.become(processing(state.flushWrites().startTransaction()))
      fut
    } else {
      val fut = publishSingleRecord(senders, records.toVector.head)
      context.become(processing(state.flushWrites()))
      fut
    }
    futureMsg.pipeTo(self)(sender())
  }

  private def handle(abortTransactionFailed: AbortTransactionFailed): Unit = {
    log.error(
      s"KafkaProducerActor partition $assignedPartition saw an error aborting transaction, will recreate the producer.",
      abortTransactionFailed.abortTransactionException)
    abortTransactionFailed.originalSenders.foreach(s => s.actorRef ! KafkaProducerActor.PublishFailure(s.trackingId, abortTransactionFailed.originalException))
    restartPublisher()
  }

  private def stopPublisher(): Unit = {
    closePublisher()
    context.stop(self)
  }

  private def restartPublisher(): Unit = {
    closeAndRecreatePublisher()
    context.system.scheduler.scheduleOnce(10.milliseconds, self, InitTransactions)
    context.become(uninitialized(None))
  }

  private def producerFenced(exception: ProducerFencedException): Unit = {
    val producerFencedErrorLog = s"KafkaProducerActor partition $assignedPartition tried to commit a transaction, but was " +
      s"fenced out by another producer instance. This instance of the producer for the assigned partition will shut down in favor of the " +
      s"newer producer for this partition.  If this message persists, check that two independent application clusters are not using the same " +
      s"transactional id prefix of [$transactionalId] for the same Kafka cluster."
    log.error(producerFencedErrorLog)
    self ! ProducerFenced(exception)
  }

  private def maybeRestartFencedProducer(fenced: ProducerFenced): Unit = {
    implicit val timeout: Timeout = Timeout(10.seconds)
    partitionTracker.getPartitionAssignments
      .map { assignments =>
        if (assignments.topicPartitionsToHosts.get(assignedPartition).exists(isHostPortThisNode)) {
          log.info(s"KafkaProducerActor partition $assignedPartition restarting because this instance is still responsible for the partition.")
          RestartProducer
        } else {
          log.info(s"KafkaProducerActor partition $assignedPartition shutting down because this instance is no longer responsible for the partition")
          ShutdownProducer
        }
      }
      .recover { case e =>
        log.warn("Exception while attempting to check if the producer could be restarted after being fenced, will check again.", e)
        fenced
      }
      .pipeTo(self)
  }

  private def handle(state: KafkaProducerActorState, isAggregateStateCurrent: IsAggregateStateCurrent): Unit = {
    val aggregateId = isAggregateStateCurrent.aggregateId
    val noRecordsInFlight = state.inFlightForAggregate(aggregateId).isEmpty
    sender() ! noRecordsInFlight

    if (noRecordsInFlight) {
      rates.current.mark()
    } else {
      rates.notCurrent.mark()
    }
  }

  private def doHealthCheck(initialFenceTime: Instant): Unit = {
    val timeSinceFenceMs = Instant.now.toEpochMilli - initialFenceTime.toEpochMilli
    val healthStatus = if (timeSinceFenceMs >= producerFencedStateUnhealthyReportTime.toMillis) {
      HealthCheckStatus.DOWN
    } else {
      HealthCheckStatus.UP
    }
    val healthCheck = HealthCheck(
      name = "producer-actor",
      id = assignedTopicPartitionKey,
      status = healthStatus,
      details = Some(Map("state" -> "fenced", "millisecondsSinceFenced" -> timeSinceFenceMs.toString)))
    sender() ! healthCheck
  }

  private def doHealthCheck(state: KafkaProducerActorState): Unit = {
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
      details = Some(
        Map(
          "inFlight" -> state.inFlight.size.toString,
          "pendingWrites" -> state.pendingWrites.size.toString,
          "currentTransactionTimeMillis" -> state.currentTransactionTimeMillis.toString,
          "state" -> "processing")))
    sender() ! healthCheck
  }
}

private[internal] object KafkaProducerActorState {
  def empty: KafkaProducerActorState = {
    KafkaProducerActorState(Seq.empty, Seq.empty, transactionInProgressSince = None)
  }
}
// TODO optimize:
//  Add in a warning if state gets too large
private[internal] case class KafkaProducerActorState(
    inFlight: Seq[KafkaRecordMetadata[String]],
    pendingWrites: Seq[KafkaProducerActorImpl.PublishWithSender],
    transactionInProgressSince: Option[Instant],
    trackers: Seq[PublishTrackerWithExpiry] = Seq.empty,
    configuredTrackerTimeout: FiniteDuration = TimeoutConfig.PublisherActor.trackerTimeout,
    lastTransactionInProgressWarningTime: Instant = Instant.ofEpochMilli(0L)) {

  import KafkaProducerActorImpl._

  private val log: Logger = LoggerFactory.getLogger(getClass)

  def transactionInProgress: Boolean = transactionInProgressSince.nonEmpty
  def currentTransactionTimeMillis: Long = {
    transactionInProgressSince.map(since => Instant.now.minusMillis(since.toEpochMilli).toEpochMilli).getOrElse(0L)
  }

  def inFlightByKey: Map[String, Seq[KafkaRecordMetadata[String]]] = {
    inFlight.groupBy(_.key.getOrElse(""))
  }

  def inFlightForAggregate(aggregateId: String): Seq[KafkaRecordMetadata[String]] = {
    inFlightByKey.getOrElse(aggregateId, Seq.empty)
  }

  def addPendingWrites(sender: ActorRef, publish: Publish): KafkaProducerActorState = {
    val newWriteRequest = PublishWithSender(SenderWithTrackingId(sender, publish.batchId), publish)
    this.copy(pendingWrites = pendingWrites :+ newWriteRequest)
  }

  def updateSenderForPendingWrite(sender: ActorRef, publish: Publish): KafkaProducerActorState = {
    val newWriteRequest = PublishWithSender(SenderWithTrackingId(sender, publish.batchId), publish)
    val maybeFound = pendingWrites.find(write => write.publish.batchId == publish.batchId)
    val index = maybeFound.map(f => pendingWrites.indexOf(f)).getOrElse(-1)
    if (index != -1) {
      this.copy(pendingWrites = pendingWrites.updated(index, newWriteRequest))
    } else {
      this.copy()
    }
  }

  def tracker(id: UUID): Option[PublishTrackerWithExpiry] = {
    trackers
      .find(t => t.tracker.requestId == id)
      .map(t => {
        if (t.tracker.isActive) {
          PublishTrackerWithExpiry(t.tracker, expired = false)
        } else {
          PublishTrackerWithExpiry(t.tracker, expired = true)
        }
      })
  }

  def hasPendingWrites(id: UUID): Boolean = {
    pendingWrites.exists(p => p.publish.batchId == id)
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

  def startTracking(msg: Publish, timeout: FiniteDuration): KafkaProducerActorState = {
    this.copy(trackers = trackers :+ PublishTrackerWithExpiry(tracker = PublishTracker(msg.batchId, msg.state, msg.eventsToPublish, timeout), expired = false))
  }

  def stopTracking(msg: Publish): KafkaProducerActorState = {
    this.stopTracking(msg.batchId)
  }

  def clearExpiredTrackers(): KafkaProducerActorState = {
    this.copy(trackers = trackers.filter(tracker => tracker.tracker.isActive))
  }

  def stopTracking(trackingId: UUID): KafkaProducerActorState = {
    this.copy(trackers = trackers.filterNot(t => t.tracker.requestId == trackingId))
  }

  def stopTrackers(trackersToStop: Seq[PublishTrackerWithExpiry]): KafkaProducerActorState = {
    val updatedTrackers: Seq[PublishTrackerWithExpiry] = trackers.filterNot(t => trackersToStop.contains(t))
    this.copy(trackers = updatedTrackers)
  }

  def addInFlight(recordMetadata: Seq[KafkaRecordMetadata[String]]): KafkaProducerActorState = {
    val newTotalInFlight = inFlight ++ recordMetadata
    val newInFlight = newTotalInFlight.groupBy(_.key).values.map(_.maxBy(_.wrapped.offset()))

    this.copy(inFlight = newInFlight.toSeq)
  }

  def processedUpTo(kTableProgressUpdate: KTableProgressUpdate): KafkaProducerActorState = {
    val kTableCurrentOffset = kTableProgressUpdate.lagInfo.currentOffsetPosition
    val topicPartition = kTableProgressUpdate.topicPartition
    val processedRecordsFromPartition = inFlight.filter(record => record.wrapped.offset() <= kTableCurrentOffset)

    if (processedRecordsFromPartition.nonEmpty) {
      val processedOffsets = processedRecordsFromPartition.map(_.wrapped.offset())
      log.trace(
        s"${topicPartition.topic}:${topicPartition.partition} processed up to offset $kTableCurrentOffset. " +
          s"Outstanding offsets that were processed are [${processedOffsets.min} -> ${processedOffsets.max}]")
    }
    val newInFlight = inFlight.filterNot(processedRecordsFromPartition.contains)

    copy(inFlight = newInFlight)
  }
}

trait KTableLagChecker {
  def getConsumerGroupLag(assignedPartition: TopicPartition): Option[LagInfo]
}
class KTableLagCheckerImpl(consumerGroup: String, adminClient: KafkaAdminClient) extends KTableLagChecker {
  def getConsumerGroupLag(assignedPartition: TopicPartition): Option[LagInfo] = {
    adminClient.consumerLag(consumerGroup, List(assignedPartition), new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)).get(assignedPartition)
  }
}

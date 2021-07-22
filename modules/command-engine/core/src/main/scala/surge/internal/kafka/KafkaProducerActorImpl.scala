// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import akka.actor.{ ActorRef, NoSerializationVerificationNeeded, Stash, Status, Timers }
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory }
import io.opentelemetry.api.trace.Tracer
import org.apache.kafka.clients.producer.{ ProducerConfig, ProducerRecord }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ AuthorizationException, ProducerFencedException }
import org.slf4j.{ Logger, LoggerFactory }
import surge.core.KafkaProducerActor
import surge.health.{ HealthSignalBusTrait, HealthyPublisher }
import surge.internal.akka.ActorWithTracing
import surge.internal.akka.cluster.ActorHostAwareness
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.kafka.streams.HealthyActor.GetHealth
import surge.kafka.streams.{ AggregateStateStoreKafkaStreams, HealthCheck, HealthCheckStatus }
import surge.kafka.{ KafkaBytesProducer, KafkaRecordMetadata, KafkaTopicTrait, LagInfo }
import surge.metrics.{ MetricInfo, Metrics, Rate, Timer }

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

object KafkaProducerActorImpl {
  val KAFKA_PRODUCER_KTABLE_ERROR_SIGNAL_NAME: String = "kafka.producer.actor.ktable.error"
  val KAFKA_PRODUCER_ABORT_TX_FAILED_SIGNAL_NAME: String = "kafka.producer.actor.abort.tx.failed"
  val KAFKA_PRODUCER_FENCED_SIGNAL_NAME: String = "kafka.producer.actor.fenced"

  sealed trait KafkaProducerActorMessage extends NoSerializationVerificationNeeded
  case class Publish(state: KafkaProducerActor.MessageToPublish, eventsToPublish: Seq[KafkaProducerActor.MessageToPublish]) extends KafkaProducerActorMessage
  case class IsAggregateStateCurrent(aggregateId: String) extends KafkaProducerActorMessage

  object AggregateStateRates {
    def apply(aggregateName: String, metrics: Metrics): AggregateStateRates = AggregateStateRates(
      current = metrics.rate(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.aggregate-state-current-rate",
          description = "The per-second rate of aggregates that are up-to-date in and can be loaded immediately from the KTable",
          tags = Map("aggregate" -> aggregateName))),
      notCurrent = metrics.rate(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.aggregate-state-not-current-rate",
          description = "The per-second rate of aggregates that are not up-to-date in the KTable and must wait to be loaded",
          tags = Map("aggregate" -> aggregateName))))

  }
  case class AggregateStateRates(current: Rate, notCurrent: Rate)

  sealed trait InternalMessage extends NoSerializationVerificationNeeded
  case class EventsPublished(originalSenders: Seq[ActorRef], recordMetadata: Seq[KafkaRecordMetadata[String]]) extends InternalMessage
  case class EventsFailedToPublish(originalSenders: Seq[ActorRef], reason: Throwable) extends InternalMessage
  case class AbortTransactionFailed(originalSenders: Seq[ActorRef], abortTransactionException: Throwable, originalException: Throwable) extends InternalMessage
  case object InitTransactions extends InternalMessage
  case object InitTransactionSuccess extends InternalMessage
  case object FailedToInitTransactions extends InternalMessage
  case object FlushMessages extends InternalMessage
  case class PublishWithSender(sender: ActorRef, publish: Publish) extends InternalMessage
  case class PendingInitialization(actor: ActorRef, key: String, expiration: Instant) extends InternalMessage
  case class KTableProgressUpdate(topicPartition: TopicPartition, lagInfo: LagInfo) extends InternalMessage
  case class ProducerFenced(exception: ProducerFencedException) extends InternalMessage
  case object RestartProducer extends InternalMessage
  case object ShutdownProducer extends InternalMessage
}
class KafkaProducerActorImpl(
    assignedPartition: TopicPartition,
    metrics: Metrics,
    producerContext: ProducerActorContext,
    kStreams: AggregateStateStoreKafkaStreams[_],
    partitionTracker: KafkaConsumerPartitionAssignmentTracker,
    override val signalBus: HealthSignalBusTrait,
    config: Config = ConfigFactory.load(),
    kafkaProducerOverride: Option[KafkaBytesProducer] = None)
    extends ActorWithTracing
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
  private val publisherBatchSize = config.getInt("kafka.publisher.batch-size")
  private val publisherLingerMs = config.getInt("kafka.publisher.linger-ms")
  private val publisherCompression = config.getString("kafka.publisher.compression-type")
  private val publisherTransactionTimeoutMs = config.getString("kafka.publisher.transaction-timeout-ms")
  private val ktableCheckInterval = config.getDuration("kafka.publisher.ktable-check-interval").toMillis.milliseconds
  private val brokers = config.getString("kafka.brokers").split(",").toVector
  private val enableMetrics = config.getBoolean("surge.producer.enable-kafka-metrics")
  private val initTransactionAuthzExceptionRetryDuration =
    config.getDuration("kafka.publisher.init-transactions.authz-exception-retry-time").toMillis.millis.toCoarsest
  private val initTransactionOtherExceptionRetryDuration =
    config.getDuration("kafka.publisher.init-transactions.other-exception-retry-time").toMillis.millis.toCoarsest

  private val transactionalId = s"$transactionalIdPrefix-${assignedPartition.topic()}-${assignedPartition.partition()}"
  private val kafkaPublisherMetricsName = transactionalId

  //noinspection ActorMutableStateInspection
  private var kafkaPublisher = getPublisher

  override val tracer: Tracer = producerContext.tracer

  private val kafkaPublisherTimer: Timer = metrics.timer(
    MetricInfo(
      s"surge.${aggregateName.toLowerCase()}.kafka-write-timer",
      "Average time in milliseconds that it takes the publisher to write a batch of messages (events & state) to Kafka",
      tags = Map("aggregate" -> aggregateName)))
  private implicit val rates: AggregateStateRates = AggregateStateRates(aggregateName, metrics)

  context.system.scheduler.scheduleOnce(10.milliseconds, self, InitTransactions)
  context.system.scheduler.scheduleWithFixedDelay(flushInterval, flushInterval, self, FlushMessages)
  context.system.scheduler.scheduleAtFixedRate(ktableCheckInterval, ktableCheckInterval)(() => checkKTableProgress())

  private def getPublisher: KafkaBytesProducer = {
    kafkaProducerOverride.getOrElse(newPublisher())
  }

  private def newPublisher(): KafkaBytesProducer = {

    object PoisonTopic extends KafkaTopicTrait {
      def name = throw new IllegalStateException("there is no topic")
    }

    val kafkaConfig = Map[String, String](
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true.toString,
      ProducerConfig.BATCH_SIZE_CONFIG -> publisherBatchSize.toString,
      ProducerConfig.LINGER_MS_CONFIG -> publisherLingerMs.toString,
      ProducerConfig.COMPRESSION_TYPE_CONFIG -> publisherCompression,
      ProducerConfig.TRANSACTION_TIMEOUT_CONFIG -> publisherTransactionTimeoutMs,
      ProducerConfig.TRANSACTIONAL_ID_CONFIG -> transactionalId)

    // Set up the producer on the events topic so the partitioner can partition automatically on the events topic since we manually set the partition for the
    // aggregate state topic record and the events topic could have a different number of partitions
    val producer = KafkaBytesProducer(brokers, eventsTopicOpt.getOrElse(PoisonTopic), partitioner, kafkaConfig)
    if (enableMetrics) {
      metrics.registerKafkaMetrics(kafkaPublisherMetricsName, () => producer.producer.metrics)
    }
    producer
  }

  override def receive: Receive = uninitialized

  override def signalMetadata(): Map[String, String] = {
    Map[String, String](elems = "aggregateName" -> aggregateName)
  }

  private def uninitialized: Receive = {
    case InitTransactions => initializeTransactions()
    case InitTransactionSuccess =>
      unstashAll()
      context.become(waitingForKTableIndexing())
    case FlushMessages              => // Ignore from this state
    case GetHealth                  => doHealthCheck()
    case _: KTableProgressUpdate    => stash() // process only AFTER flush message is sent
    case _: Publish                 => stash()
    case _: IsAggregateStateCurrent => sender().tell(false, self)
  }

  private def waitingForKTableIndexing(): Receive = {
    case msg: KTableProgressUpdate  => handleFromWaitingForKTableIndexingState(msg)
    case FlushMessages              => // Ignore from this state
    case GetHealth                  => doHealthCheck()
    case _: Publish                 => stash()
    case _: IsAggregateStateCurrent => sender().tell(false, self)
  }

  private def fenced(state: KafkaProducerActorState): Receive = {
    case msg: ProducerFenced => maybeRestartFencedProducer(state, msg)
    case msg: EventsFailedToPublish =>
      context.become(fenced(state.completeTransaction()))
      msg.originalSenders.foreach(_ ! KafkaProducerActor.PublishFailure(msg.reason))
    case RestartProducer  => restartPublisher()
    case ShutdownProducer => context.stop(self)
    case _                => stash()
  }

  private def processing(state: KafkaProducerActorState): Receive = {
    case msg: InternalMessage                                  => handleProcessingInternalMessage(msg, state)
    case msg: KafkaProducerActorImpl.KafkaProducerActorMessage => handleProcessingProducerMessage(msg, state)
    case GetHealth                                             => doHealthCheck(state)
    case Status.Failure(e) =>
      log.error(s"Saw unhandled exception in producer for $assignedPartition", e)
  }

  private def handleProcessingProducerMessage(message: KafkaProducerActorImpl.KafkaProducerActorMessage, state: KafkaProducerActorState): Unit = {
    message match {
      case msg: Publish                 => handle(state, msg)
      case msg: IsAggregateStateCurrent => handle(state, msg)
      case other                        => unhandled(other)
    }
  }

  private def handleProcessingInternalMessage(message: InternalMessage, state: KafkaProducerActorState): Unit = {
    message match {
      case msg: KTableProgressUpdate   => handle(state, msg)
      case msg: EventsPublished        => handle(state, msg)
      case msg: EventsFailedToPublish  => handleFailedToPublish(state, msg)
      case FlushMessages               => handleFlushMessages(state)
      case msg: AbortTransactionFailed => handle(msg)
      case msg: ProducerFenced =>
        context.become(fenced(state))
        self ! msg
      case other => unhandled(other)
    }
  }

  private def checkKTableProgress(): Unit = {
    kStreams.partitionLag(assignedPartition).foreach { lagOpt =>
      lagOpt.foreach { lag =>
        self ! KTableProgressUpdate(assignedPartition, lag)
      }
    }
  }

  private def initializeTransactions(): Unit = {
    kafkaPublisher
      .initTransactions()
      .map { _ =>
        log.debug(s"KafkaPublisherActor transactions successfully initialized: $assignedPartition")
        self ! InitTransactionSuccess
      }
      .recover { case err: Throwable =>
        val retryTime = initTransactionsRetryTimeFromError(err)
        log.error(
          s"KafkaPublisherActor failed to initialize kafka transactions with transactional id [$transactionalId] " +
            s"for partition [$assignedPartition], retrying in $retryTime",
          err)
        closeAndRecreatePublisher()
        context.system.scheduler.scheduleOnce(retryTime, self, InitTransactions)
      }
  }

  private def initTransactionsRetryTimeFromError(err: Throwable): FiniteDuration = {
    err match {
      case _: AuthorizationException => initTransactionAuthzExceptionRetryDuration
      case _                         => initTransactionOtherExceptionRetryDuration
    }
  }

  private def closeAndRecreatePublisher(): Unit = {
    if (enableMetrics) {
      metrics.unregisterKafkaMetric(kafkaPublisherMetricsName)
    }
    Try(kafkaPublisher.close())
    kafkaPublisher = getPublisher
  }

  private def handle(state: KafkaProducerActorState, publish: Publish): Unit = {
    context.become(processing(state.addPendingWrites(sender(), publish)))
  }

  private def handle(state: KafkaProducerActorState, kTableProgressUpdate: KTableProgressUpdate): Unit = {
    context.become(processing(state.processedUpTo(kTableProgressUpdate)))
  }

  private def handleFromWaitingForKTableIndexingState(kTableProgressUpdate: KTableProgressUpdate): Unit = {
    val currentLag = kTableProgressUpdate.lagInfo.offsetLag
    if (currentLag == 0L) {
      log.info(s"KafkaPublisherActor partition {} is fully up to date on processing", assignedPartition)
      unstashAll()
      context.become(processing(KafkaProducerActorState.empty))
    } else {
      log.debug("Producer actor still waiting for KTable to finish indexing, current lag is {}", kTableProgressUpdate.lagInfo.offsetLag)
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

  // FIXME need to open a GH issue for this warning
  private var lastTransactionInProgressWarningTime: Instant = Instant.ofEpochMilli(0L)
  private val transactionTimeWarningThreshold = flushInterval.toMillis * 4
  private val eventsPublishedRate: Rate = metrics.rate(
    MetricInfo(
      name = s"surge.${aggregateName.toLowerCase()}.event-publish-rate",
      description = "The per-second rate at which this aggregate attempts to publish events to Kafka",
      tags = Map("aggregate" -> aggregateName)))
  private def handleFlushMessages(state: KafkaProducerActorState): Unit = {
    if (state.transactionInProgress) {
      if (state.currentTransactionTimeMillis >= transactionTimeWarningThreshold &&
        lastTransactionInProgressWarningTime.plusSeconds(1L).isBefore(Instant.now())) {
        lastTransactionInProgressWarningTime = Instant.now
        log.warn(
          s"KafkaPublisherActor partition {} tried to flush, but another transaction is already in progress. " +
            s"The previous transaction has been in progress for {} milliseconds. If the time to complete the previous transaction continues to grow " +
            s"that typically indicates slowness in the Kafka brokers.",
          assignedPartition,
          state.currentTransactionTimeMillis)
      }
    } else if (state.pendingWrites.nonEmpty) {
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
        case Failure(f: ProducerFencedException) =>
          producerFenced(state, f)
          Future.successful(EventsFailedToPublish(senders, f))
        case Failure(err) =>
          log.error(s"KafkaPublisherActor partition $assignedPartition there was an error beginning transaction", err)
          Future.successful(EventsFailedToPublish(senders, err))
        case _ =>
          Future
            .sequence(kafkaPublisher.putRecords(records))
            .map { recordMeta =>
              log.debug(s"KafkaPublisherActor partition {} committing transaction", assignedPartition)
              kafkaPublisher.commitTransaction()
              EventsPublished(senders, recordMeta.filter(_.wrapped.topic() == stateTopic.name))
            }
            .recover {
              case e: ProducerFencedException =>
                producerFenced(state, e)
                EventsFailedToPublish(senders, e)
              case e =>
                log.error(s"KafkaPublisherActor partition $assignedPartition got error while trying to publish to Kafka", e)
                Try(kafkaPublisher.abortTransaction()) match {
                  case Success(_) =>
                    EventsFailedToPublish(senders, e)
                  case Failure(exception) =>
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
    restartPublisher()
  }

  private def restartPublisher(): Unit = {
    closeAndRecreatePublisher()
    context.system.scheduler.scheduleOnce(10.milliseconds, self, InitTransactions)
    context.become(uninitialized)
  }

  private def producerFenced(state: KafkaProducerActorState, exception: ProducerFencedException): Unit = {
    val producerFencedErrorLog = s"KafkaPublisherActor partition $assignedPartition tried to commit a transaction, but was " +
      s"fenced out by another producer instance. This instance of the producer for the assigned partition will shut down in favor of the " +
      s"newer producer for this partition.  If this message persists, check that two independent application clusters are not using the same " +
      s"transactional id prefix of [$transactionalId] for the same Kafka cluster."
    log.error(producerFencedErrorLog)
    self ! ProducerFenced(exception)
  }

  private def maybeRestartFencedProducer(state: KafkaProducerActorState, fenced: ProducerFenced): Unit = {
    implicit val timeout: Timeout = Timeout(10.seconds)
    partitionTracker.getPartitionAssignments
      .map { assignments =>
        if (assignments.topicPartitionsToHosts.get(assignedPartition).exists(isHostPortThisNode)) {
          log.info(s"KafkaPublisherActor partition $assignedPartition restarting because this instance is still responsible for the partition.")
          RestartProducer
        } else {
          log.info(s"KafkaPublisherActor partition $assignedPartition shutting down because this instance is no longer responsible for the partition")
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

  private def doHealthCheck(): Unit = {
    val healthCheck = HealthCheck(name = "producer-actor", id = assignedTopicPartitionKey, status = HealthCheckStatus.UP)
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
          "currentTransactionTimeMillis" -> state.currentTransactionTimeMillis.toString)))
    sender() ! healthCheck
  }
}

private[internal] object KafkaProducerActorState {
  def empty(implicit sender: ActorRef, rates: KafkaProducerActorImpl.AggregateStateRates): KafkaProducerActorState = {
    KafkaProducerActorState(Seq.empty, Seq.empty, transactionInProgressSince = None, sender = sender, rates = rates)
  }
}
// TODO optimize:
//  Add in a warning if state gets too large
private[internal] case class KafkaProducerActorState(
    inFlight: Seq[KafkaRecordMetadata[String]],
    pendingWrites: Seq[KafkaProducerActorImpl.PublishWithSender],
    transactionInProgressSince: Option[Instant],
    sender: ActorRef,
    rates: KafkaProducerActorImpl.AggregateStateRates) {

  import KafkaProducerActorImpl._

  private implicit val senderActor: ActorRef = sender
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

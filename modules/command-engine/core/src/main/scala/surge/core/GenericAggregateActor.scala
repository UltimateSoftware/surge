// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import java.time.Instant
import java.util.concurrent.Executors

import akka.actor.{ Actor, NoSerializationVerificationNeeded, Props, ReceiveTimeout, Stash }
import akka.pattern.pipe
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.header.internals.RecordHeaders
import org.slf4j.LoggerFactory
import play.api.libs.json.JsValue
import surge.akka.cluster.{ JacksonSerializable, Passivate }
import surge.config.{ RetryConfig, TimeoutConfig }
import surge.kafka.streams.AggregateStateStoreKafkaStreams
import surge.metrics.{ MetricsProvider, Timer }
import surge.scala.core.kafka.HeadersHelper
import surge.scala.core.validations.ValidationError

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

private[surge] object GenericAggregateActor {
  private val config = ConfigFactory.load()
  private val serializationThreadPoolSize = config.getInt("surge.serialization.thread-pool-size")
  val serializationExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(serializationThreadPoolSize))

  def props[Agg, Command, Event](
    aggregateId: String,
    businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event],
    kafkaProducerActor: KafkaProducerActor[Agg, Event],
    metrics: GenericAggregateActorMetrics,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue]): Props = {
    Props(new GenericAggregateActor(aggregateId, kafkaProducerActor, businessLogic, metrics, kafkaStreamsCommand))
      .withDispatcher("generic-aggregate-actor-dispatcher")
  }

  sealed trait RoutableMessage extends JacksonSerializable {
    def aggregateId: String
  }
  object RoutableMessage {
    def extractEntityId: PartialFunction[Any, String] = {
      case routableMessage: RoutableMessage => routableMessage.aggregateId
    }
  }

  case class CommandEnvelope[Cmd](
      aggregateId: String,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "cmdType", visible = true) command: Cmd) extends RoutableMessage
  case class GetState(aggregateId: String) extends RoutableMessage
  case class ApplyEventEnvelope[Event](
      aggregateId: String,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "eventType", visible = true) event: Event) extends RoutableMessage

  // FIXME Kotlin code can't properly match against a StateResponse/CommandSuccess when we use jackson serialization
  case class StateResponse[Agg](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateType", visible = true) aggregateState: Option[Agg])

  // TODO Revisit serialization of these responses as well
  sealed trait CommandResponse
  case class CommandFailure(validationError: Seq[ValidationError]) extends CommandResponse
  case class CommandError(exception: Throwable) extends CommandResponse with NoSerializationVerificationNeeded // FIXME remove NoSerializationVerification

  case class CommandSuccess[Agg](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateType", visible = true) aggregateState: Option[Agg]) extends CommandResponse

  def createMetrics(metricsProvider: MetricsProvider, aggregateName: String): GenericAggregateActorMetrics = {
    GenericAggregateActorMetrics(
      stateInitializationTimer = metricsProvider.createTimer(s"${aggregateName}ActorStateInitializationTimer"),
      aggregateDeserializationTimer = metricsProvider.createTimer(s"${aggregateName}AggregateStateDeserializationTimer"),
      commandHandlingTimer = metricsProvider.createTimer(s"${aggregateName}CommandHandlingTimer"),
      eventHandlingTimer = metricsProvider.createTimer(s"${aggregateName}EventHandlingTimer"),
      serializeStateTimer = metricsProvider.createTimer(s"${aggregateName}AggregateStateSerializationTimer"),
      serializeEventTimer = metricsProvider.createTimer(s"${aggregateName}EventSerializationTimer"),
      eventPublishTimer = metricsProvider.createTimer(s"${aggregateName}EventPublishTimer"))
  }

  case class GenericAggregateActorMetrics(
      stateInitializationTimer: Timer,
      aggregateDeserializationTimer: Timer,
      commandHandlingTimer: Timer,
      eventHandlingTimer: Timer,
      serializeStateTimer: Timer,
      serializeEventTimer: Timer,
      eventPublishTimer: Timer)
  case object Stop
}

/**
 * The GenericAggregateActor represents a business logic aggregate.  It handles commands
 * using the given business logic and emits events and state to Kafka.  When run as part
 * of the Surge command engine, there is exactly one GenericAggregateActor responsible for
 * each aggregate.
 *
 * @param aggregateId The aggregate id for the aggregate represented by this actor
 * @param kafkaProducerActor A reference to the Kafka producer actor for the shard that
 *                           this actor lives within.  This producer is used to send state
 *                           updates and events to Kafka.
 * @param businessLogic Domain business logic used to determine the events to emit and how to update
 *                      state based on new events.
 * @param metrics A reference to the metrics interface for exposing internal actor metrics
 * @param kafkaStreamsCommand A reference to the aggregate state store in Kafka Streams used
 *                            to fetch the state of an aggregate on initialization
 * @tparam Agg Type of aggregate represented by this actor
 * @tparam Command Type of command handled by the aggregate
 * @tparam Event Type of events emitted by the aggregate
 */
private[surge] class GenericAggregateActor[Agg, Command, Event](
    aggregateId: String,
    kafkaProducerActor: KafkaProducerActor[Agg, Event],
    businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event],
    metrics: GenericAggregateActor.GenericAggregateActorMetrics,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue]) extends Actor with Stash {
  import GenericAggregateActor._
  import context.dispatcher

  private val config = ConfigFactory.load()
  private val maxProducerFailureRetries: Int = config.getInt("surge.aggregate-actor.publish-failure-max-retries")

  private sealed trait Internal extends NoSerializationVerificationNeeded
  private case class InitializeWithState(stateOpt: Option[Agg]) extends Internal
  private case class PersistenceSuccess(newState: InternalActorState, startTime: Instant) extends Internal
  private case class PersistenceFailure(newState: InternalActorState, reason: Throwable, numberOfFailures: Int,
      serializedEvents: Seq[KafkaProducerActor.MessageToPublish],
      serializedState: KafkaProducerActor.MessageToPublish, startTime: Instant) extends Internal
  private case class EventPublishTimedOut(reason: Throwable, startTime: Instant) extends Internal

  private case class InternalActorState(stateOpt: Option[Agg])

  private val receiveTimeout = TimeoutConfig.AggregateActor.idleTimeout

  private val log = LoggerFactory.getLogger(getClass)
  private val publishStateOnly: Boolean = businessLogic.kafka.publishStateOnly

  override def preStart(): Unit = {
    initializeState(initializationAttempts = 0)
    super.preStart()
  }

  override def receive: Receive = uninitialized

  private def freeToProcess(state: InternalActorState): Receive = {
    case msg: CommandEnvelope[Command]  => handle(state, msg)
    case msg: ApplyEventEnvelope[Event] => handle(state, msg)
    case _: GetState                    => sender() ! StateResponse(state.stateOpt)
    case ReceiveTimeout                 => handlePassivate()
    case Stop                           => handleStop()
  }

  private def persistingEvents(state: InternalActorState): Receive = {
    case msg: PersistenceSuccess   => handle(state, msg)
    case msg: CommandFailure       => handle(state, msg)
    case msg: CommandError         => handleCommandError(state, msg)
    case msg: PersistenceFailure   => handleFailedToPersist(state, msg)
    case msg: EventPublishTimedOut => handlePersistenceTimedOut(state, msg)
    case ReceiveTimeout            => // Ignore and drop ReceiveTimeout messages from this state
    case other =>
      log.info(s"Aggregate actor for $aggregateId stashing a message with class [{}] from the 'persistingEvents' state", other.getClass)
      stash()
  }

  private def uninitialized: Receive = {
    case msg: InitializeWithState => handle(msg)
    case ReceiveTimeout =>
      // Ignore and drop ReceiveTimeout messages from this state
      log.warn(s"Aggregate actor for $aggregateId received a ReceiveTimeout message in uninitialized state. " +
        "This should not happen and is likely a logic error. Dropping the ReceiveTimeout message.")
    case other =>
      log.debug(s"Aggregate actor for $aggregateId stashing a message with class [{}] from the 'uninitialized' state", other.getClass)
      stash()
  }

  private def handle(state: InternalActorState, commandEnvelope: CommandEnvelope[Command]): Unit = {
    context.setReceiveTimeout(Duration.Inf)
    context.become(persistingEvents(state))

    val futureEventsPersisted = processCommand(state, commandEnvelope) match {
      case Right(Success(events)) =>
        val newState = events.foldLeft(state.stateOpt) { (state, evt) =>
          metrics.eventHandlingTimer.time(businessLogic.model.handleEvent(state, evt))
        }
        val serializedStateFut = serializeState(newState)

        val serializedEventsFut = if (publishStateOnly) {
          Future.successful(Seq.empty)
        } else {
          serializeEvents(events)
        }

        for {
          serializedEvents <- serializedEventsFut
          serializedState <- serializedStateFut
          publishResult <- doPublish(state.copy(stateOpt = newState), serializedEvents, serializedState, startTime = Instant.now, didStateChange = state.stateOpt != newState)
        } yield {
          publishResult
        }
      case Right(Failure(exception)) =>
        Future.successful(CommandError(exception))
      case Left(validationErrors) =>
        Future.successful(CommandFailure(validationErrors))
    }

    futureEventsPersisted.pipeTo(self)(sender())
  }

  private def handle(state: InternalActorState, applyEventEnvelope: ApplyEventEnvelope[Event]): Unit = {
    context.setReceiveTimeout(Duration.Inf)
    context.become(persistingEvents(state))

    val newState: Option[Agg] = businessLogic.model.handleEvent(state.stateOpt, applyEventEnvelope.event)
    val futureStatePersisted = serializeState(newState).flatMap { serializedState =>
      doPublish(state.copy(stateOpt = newState), serializedEvents = Seq.empty,
        serializedState = serializedState, startTime = Instant.now, didStateChange = state.stateOpt != newState)
    }
    futureStatePersisted.pipeTo(self)(sender())
  }

  private def doPublish(state: InternalActorState, serializedEvents: Seq[KafkaProducerActor.MessageToPublish],
    serializedState: KafkaProducerActor.MessageToPublish, currentFailureCount: Int = 0, startTime: Instant,
    didStateChange: Boolean): Future[Internal] = {
    log.trace("GenericAggregateActor for {} publishing messages", aggregateId)
    if ((serializedEvents.isEmpty && !didStateChange)) {
      Future.successful(PersistenceSuccess(state, startTime))
    } else {
      kafkaProducerActor.publish(aggregateId = aggregateId, state = serializedState, events = serializedEvents).map {
        case KafkaProducerActor.PublishSuccess    ⇒ PersistenceSuccess(state, startTime)
        case KafkaProducerActor.PublishFailure(t) ⇒ PersistenceFailure(state, t, currentFailureCount + 1, serializedEvents, serializedState, startTime)
      }.recover {
        case t ⇒ EventPublishTimedOut(t, startTime)
      }
    }
  }

  private def serializeEvents(events: Seq[Event]): Future[Seq[KafkaProducerActor.MessageToPublish]] = Future {
    events.map { event =>
      val serializedMessage = metrics.serializeEventTimer.time(businessLogic.writeFormatting.writeEvent(event))
      log.trace(s"Publishing event for {} {}", Seq(businessLogic.aggregateName, serializedMessage.key): _*)
      KafkaProducerActor.MessageToPublish(
        key = serializedMessage.key,
        value = serializedMessage.value,
        headers = HeadersHelper.createHeaders(serializedMessage.headers))
    }
  }(GenericAggregateActor.serializationExecutionContext)

  private def serializeState(stateValueOpt: Option[Agg]): Future[KafkaProducerActor.MessageToPublish] = Future {
    val serializedStateOpt = stateValueOpt.map { value =>
      metrics.serializeStateTimer.time(businessLogic.writeFormatting.writeState(value))
    }
    val stateValue = serializedStateOpt.map(_.value).orNull
    val stateHeaders = serializedStateOpt.map(ser => HeadersHelper.createHeaders(ser.headers)).getOrElse(new RecordHeaders())
    KafkaProducerActor.MessageToPublish(aggregateId.toString, stateValue, stateHeaders)
  }(GenericAggregateActor.serializationExecutionContext)

  // TODO can we handle this more gracefully? If we're unsure something published or not from the timeout the safest thing to do for now
  //  is to crash the actor and force reinitialization
  private def handlePersistenceTimedOut(state: InternalActorState, msg: EventPublishTimedOut): Unit = {
    log.error(s"Error while trying to publish to Kafka, crashing actor for ${businessLogic.aggregateName} $aggregateId", msg.reason)
    metrics.eventPublishTimer.recordTime(publishTimeInMillis(msg.startTime))
    sender() ! CommandError(msg.reason)
    context.stop(self)
  }

  private def handleFailedToPersist(state: InternalActorState, eventsFailedToPersist: PersistenceFailure): Unit = {
    if (eventsFailedToPersist.numberOfFailures > maxProducerFailureRetries) {
      log.error(s"Failed to publish to Kafka after ${eventsFailedToPersist.numberOfFailures} tries, crashing " +
        s"actor for ${businessLogic.aggregateName} $aggregateId", eventsFailedToPersist.reason)
      metrics.eventPublishTimer.recordTime(publishTimeInMillis(eventsFailedToPersist.startTime))
      sender() ! CommandError(eventsFailedToPersist.reason)
      context.stop(self)
    } else {
      log.warn(
        s"Failed to publish to Kafka after try #${eventsFailedToPersist.numberOfFailures}, retrying for ${businessLogic.aggregateName} $aggregateId",
        eventsFailedToPersist.reason)
      doPublish(eventsFailedToPersist.newState, eventsFailedToPersist.serializedEvents, eventsFailedToPersist.serializedState,
        eventsFailedToPersist.numberOfFailures, eventsFailedToPersist.startTime, didStateChange = true).pipeTo(self)(sender())
    }
  }

  private def publishTimeInMillis(startTime: Instant): Long = {
    Instant.now.toEpochMilli - startTime.toEpochMilli
  }

  private def processCommand(
    state: InternalActorState,
    commandEnvelope: CommandEnvelope[Command]): Either[Seq[ValidationError], Try[Seq[Event]]] = {
    metrics.commandHandlingTimer.time(Right(businessLogic.model.processCommand(state.stateOpt, commandEnvelope.command)))
  }

  private def handle(state: InternalActorState, failure: CommandFailure): Unit = {
    log.debug(s"The command for ${businessLogic.aggregateName} $aggregateId resulted in a validation error")
    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(state))

    sender() ! failure
  }

  private def handleCommandError(state: InternalActorState, error: CommandError): Unit = {
    log.debug(s"The command for ${businessLogic.aggregateName} $aggregateId resulted in an error", error.exception)
    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(state))

    sender() ! error
  }

  private def handle(state: InternalActorState, msg: PersistenceSuccess): Unit = {
    metrics.eventPublishTimer.recordTime(publishTimeInMillis(msg.startTime))

    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(msg.newState))

    val cmdSuccess = CommandSuccess(msg.newState.stateOpt)
    sender() ! cmdSuccess
    unstashAll()
  }

  private def handle(initializeWithState: InitializeWithState): Unit = {
    log.debug(s"Actor state for aggregate $aggregateId successfully initialized")
    unstashAll()

    val internalActorState = InternalActorState(
      stateOpt = initializeWithState.stateOpt)

    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(internalActorState))
  }

  private def initializeState(initializationAttempts: Int): Unit = {
    if (initializationAttempts > RetryConfig.AggregateActor.maxInitializationAttempts) {
      log.error(s"Could not initialize actor for $aggregateId after $initializationAttempts attempts.  Stopping actor")
      context.stop(self)
    } else {
      kafkaProducerActor.isAggregateStateCurrent(aggregateId).map { isStateCurrent =>
        if (isStateCurrent) {
          fetchState(initializationAttempts)
        } else {
          val retryIn = RetryConfig.AggregateActor.initializeStateInterval
          log.warn("State for {} is not up to date in Kafka streams, retrying initialization in {}", Seq(aggregateId, retryIn): _*)
          context.system.scheduler.scheduleOnce(retryIn)(initializeState(initializationAttempts + 1))
        }
      }.recover {
        case exception =>
          val retryIn = RetryConfig.AggregateActor.fetchStateRetryInterval
          log.error(s"Failed to check if ${businessLogic.aggregateName} aggregate was up to date for id $aggregateId, retrying in $retryIn", exception)
          context.system.scheduler.scheduleOnce(retryIn)(initializeState(initializationAttempts + 1))
      }
    }
  }

  private def handlePassivate(): Unit = {
    log.trace(s"GenericAggregateActor for aggregate ${businessLogic.aggregateName} $aggregateId is passivating gracefully")
    context.parent ! Passivate(Stop)
  }

  private def handleStop(): Unit = {
    log.trace(s"GenericAggregateActor for aggregate ${businessLogic.aggregateName} $aggregateId is stopping gracefully")
    context.stop(self)
  }

  private def fetchState(initializationAttempts: Int): Unit = {
    val fetchedStateFut = metrics.stateInitializationTimer.time(kafkaStreamsCommand.getAggregateBytes(aggregateId.toString))

    fetchedStateFut.map { state =>
      log.trace("GenericAggregateActor for {} fetched state", aggregateId)
      val stateOpt = state.flatMap { bodyBytes =>
        metrics.aggregateDeserializationTimer.time(businessLogic.readFormatting.readState(bodyBytes))
      }

      self ! InitializeWithState(stateOpt)
    }.recover {
      case _: NullPointerException =>
        log.error("Used null for aggregateId - this is weird. Will not try to initialize state again")
      case exception =>
        val retryIn = RetryConfig.AggregateActor.fetchStateRetryInterval
        log.error(s"Failed to initialize ${businessLogic.aggregateName} actor for aggregate $aggregateId, retrying in $retryIn", exception)
        context.system.scheduler.scheduleOnce(retryIn)(initializeState(initializationAttempts + 1))
    }
  }

}

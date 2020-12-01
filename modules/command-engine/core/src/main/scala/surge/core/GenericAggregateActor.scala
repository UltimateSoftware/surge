// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import java.time.Instant

import akka.Done
import akka.actor.{ Actor, NoSerializationVerificationNeeded, Props, ReceiveTimeout, Stash }
import akka.pattern.pipe
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.slf4j.LoggerFactory
import play.api.libs.json.JsValue
import surge.akka.cluster.{ JacksonSerializable, Passivate }
import surge.config.{ RetryConfig, TimeoutConfig }
import surge.kafka.streams.AggregateStateStoreKafkaStreams
import surge.metrics.{ MetricsProvider, Timer }
import surge.scala.core.validations.ValidationError

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

private[surge] object GenericAggregateActor {
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
      case routableMessage: RoutableMessage ⇒ routableMessage.aggregateId
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

  sealed trait CommandResponse
  case class CommandFailure(validationError: Seq[ValidationError]) extends CommandResponse
  case class CommandError(exception: Throwable) extends CommandResponse

  case class CommandSuccess[Agg](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateType", visible = true) aggregateState: Option[Agg]) extends CommandResponse

  def createMetrics(metricsProvider: MetricsProvider, aggregateName: String): GenericAggregateActorMetrics = {
    GenericAggregateActorMetrics(
      stateInitializationTimer = metricsProvider.createTimer(s"${aggregateName}ActorStateInitializationTimer"),
      aggregateDeserializationTimer = metricsProvider.createTimer(s"${aggregateName}AggregateStateDeserializationTimer"),
      commandHandlingTimer = metricsProvider.createTimer(s"${aggregateName}CommandHandlingTimer"),
      eventHandlingTimer = metricsProvider.createTimer(s"${aggregateName}EventHandlingTimer"))
  }

  case class GenericAggregateActorMetrics(
      stateInitializationTimer: Timer,
      aggregateDeserializationTimer: Timer,
      commandHandlingTimer: Timer,
      eventHandlingTimer: Timer)
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

  private sealed trait Internal extends NoSerializationVerificationNeeded
  private case class InitializeWithState(stateOpt: Option[Agg]) extends Internal
  private case class EventsSuccessfullyPersisted(newState: InternalActorState, commandReceivedTime: Instant) extends Internal

  private case class InternalActorState(stateOpt: Option[Agg])

  private val receiveTimeout = TimeoutConfig.AggregateActor.idleTimeout

  private val log = LoggerFactory.getLogger(getClass)

  override def preStart(): Unit = {
    initializeState(initializationAttempts = 0)
    super.preStart()
  }

  override def receive: Receive = uninitialized

  private def freeToProcess(state: InternalActorState): Receive = {
    case msg: CommandEnvelope[Command]  ⇒ handle(state, msg)
    case msg: ApplyEventEnvelope[Event] ⇒ handle(state, msg)
    case _: GetState                    ⇒ sender() ! StateResponse(state.stateOpt)
    case ReceiveTimeout                 ⇒ handlePassivate()
    case Stop                           ⇒ handleStop()
  }

  private def persistingEvents(state: InternalActorState): Receive = {
    case msg: EventsSuccessfullyPersisted ⇒ handle(state, msg)
    case msg: CommandFailure              ⇒ handle(state, msg)
    case msg: CommandError                ⇒ handleCommandError(state, msg)
    case ReceiveTimeout                   ⇒ // Ignore and drop ReceiveTimeout messages from this state
    case _                                ⇒ stash()
  }

  private def uninitialized: Receive = {
    case msg: InitializeWithState ⇒ handle(msg)
    case ReceiveTimeout ⇒
      // Ignore and drop ReceiveTimeout messages from this state
      log.warn(s"Aggregate actor for $aggregateId received a ReceiveTimeout message in uninitialized state. " +
        "This should not happen and is likely a logic error. Dropping the ReceiveTimeout message.")
    case _ ⇒ stash()
  }

  private def handle(state: InternalActorState, commandEnvelope: CommandEnvelope[Command]): Unit = {
    val startTime = Instant.now
    context.setReceiveTimeout(Duration.Inf)
    context.become(persistingEvents(state))

    val futureEventsPersisted = processCommand(state, commandEnvelope).flatMap {
      case Right(Success(events)) ⇒
        val newState = events.foldLeft(state.stateOpt) { (state, evt) ⇒
          metrics.eventHandlingTimer.time(businessLogic.model.handleEvent(state, evt))
        }
        val stateKeyValue = aggregateId -> newState

        log.trace("GenericAggregateActor for {} publishing messages", aggregateId)
        val publishFuture = if (events.nonEmpty) {
          kafkaProducerActor.publish(aggregateId = aggregateId, state = stateKeyValue, events = events)
        } else {
          Future.successful(Done)
        }
        publishFuture.map { _ ⇒
          val newActorState = InternalActorState(stateOpt = newState)
          EventsSuccessfullyPersisted(newActorState, startTime)
        }
      case Right(Failure(exception)) ⇒
        Future.successful(CommandError(exception))
      case Left(validationErrors) ⇒
        Future.successful(CommandFailure(validationErrors))
    }

    futureEventsPersisted.recover {
      case ex ⇒
        // On an error, we don't know if the command succeeded or not, so crash the actor and force it to reinitialize
        log.error(s"Error while trying to persist events, crashing actor for ${businessLogic.aggregateName} $aggregateId", ex)
        context.stop(self)
    }.pipeTo(self)(sender())
  }

  private def handle(state: InternalActorState, applyEventEnvelope: ApplyEventEnvelope[Event]): Unit = {
    val startTime = Instant.now
    context.setReceiveTimeout(Duration.Inf)
    context.become(persistingEvents(state))

    val newState = businessLogic.model.handleEvent(state.stateOpt, applyEventEnvelope.event)
    val stateKeyValue = aggregateId.toString -> newState

    val futureStatePersisted = kafkaProducerActor.publish(aggregateId = aggregateId, state = stateKeyValue, events = Seq.empty).map { _ ⇒
      val newActorState = InternalActorState(stateOpt = newState)
      EventsSuccessfullyPersisted(newActorState, startTime)
    }
    futureStatePersisted.recover {
      case ex ⇒
        // On an error, we don't know if the persist succeeded or not, so crash the actor and force it to reinitialize
        log.error(s"Error while trying to persist events, crashing actor for ${businessLogic.aggregateName} $aggregateId", ex)
        context.stop(self)
    }.pipeTo(self)(sender())
  }

  private def processCommand(
    state: InternalActorState,
    commandEnvelope: CommandEnvelope[Command]): Future[Either[Seq[ValidationError], Try[Seq[Event]]]] = {
    Future {
      metrics.commandHandlingTimer.time(Right(businessLogic.model.processCommand(state.stateOpt, commandEnvelope.command)))
    }
  }

  private def handle(state: InternalActorState, failure: CommandFailure): Unit = {
    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(state))

    sender() ! failure
  }

  private def handleCommandError(state: InternalActorState, error: CommandError): Unit = {
    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(state))

    sender() ! error
  }

  private def handle(state: InternalActorState, msg: EventsSuccessfullyPersisted): Unit = {
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
      kafkaProducerActor.isAggregateStateCurrent(aggregateId).map { isStateCurrent ⇒
        if (isStateCurrent) {
          fetchState(initializationAttempts)
        } else {
          val retryIn = RetryConfig.AggregateActor.initializeStateInterval
          log.warn("State for {} is not up to date in Kafka streams, retrying initialization in {}", Seq(aggregateId, retryIn): _*)
          context.system.scheduler.scheduleOnce(retryIn)(initializeState(initializationAttempts + 1))
        }
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

    fetchedStateFut.map { state ⇒
      log.trace("GenericAggregateActor for {} fetched state", aggregateId)
      val stateOpt = state.flatMap { bodyBytes ⇒
        metrics.aggregateDeserializationTimer.time(businessLogic.readFormatting.readState(bodyBytes))
      }

      self ! InitializeWithState(stateOpt)
    }.recover {
      case _: NullPointerException ⇒
        log.error("Used null for aggregateId - this is weird. Will not try to initialize state again")
      case exception ⇒
        val retryIn = RetryConfig.AggregateActor.fetchStateRetryInterval
        log.error(s"Failed to initialize ${businessLogic.aggregateName} actor for aggregate $aggregateId, retrying in $retryIn", exception)
        context.system.scheduler.scheduleOnce(retryIn)(initializeState(initializationAttempts + 1))
    }
  }

}

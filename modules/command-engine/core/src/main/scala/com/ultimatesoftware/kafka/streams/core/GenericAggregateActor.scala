// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant

import akka.Done
import akka.actor.{ Actor, Props, ReceiveTimeout, Stash }
import akka.pattern.pipe
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.ultimatesoftware.akka.cluster.{ JacksonSerializable, Passivate }
import com.ultimatesoftware.config.{ RetryConfig, TimeoutConfig }
import com.ultimatesoftware.kafka.streams.AggregateStateStoreKafkaStreams
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, Timer }
import com.ultimatesoftware.scala.core.validations._
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

private[streams] object GenericAggregateActor {
  def props[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    aggregateId: AggId,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, _, EvtMeta],
    kafkaProducerActor: KafkaProducerActor[AggId, Agg, Event, EvtMeta],
    metrics: GenericAggregateActorMetrics,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue]): Props = {
    Props(new GenericAggregateActor(aggregateId, kafkaProducerActor, businessLogic, metrics, kafkaStreamsCommand))
      .withDispatcher("generic-aggregate-actor-dispatcher")
  }

  sealed trait RoutableMessage[AggId] extends JacksonSerializable {
    def aggregateId: AggId
  }
  implicit def commandEnvelopeFormat[AggId, Cmd, CmdMeta](implicit
    aggIdTypeFormat: Format[AggId],
    cmdFormat: Format[Cmd],
    propsFormat: Format[CmdMeta]): Format[CommandEnvelope[AggId, Cmd, CmdMeta]] = {
    Json.format[CommandEnvelope[AggId, Cmd, CmdMeta]]
  }
  object RoutableMessage {
    def extractEntityId[AggIdType]: PartialFunction[Any, AggIdType] = {
      case routableMessage: RoutableMessage[AggIdType] ⇒ routableMessage.aggregateId
    }
  }

  case class CommandEnvelope[AggIdType, Cmd, CmdMeta](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateIdType", visible = true) aggregateId: AggIdType,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "cmdMetaType", visible = true) meta: CmdMeta,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "cmdType", visible = true) command: Cmd) extends RoutableMessage[AggIdType]
  case class GetState[AggIdType](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateIdType", visible = true) aggregateId: AggIdType) extends RoutableMessage[AggIdType]
  case class ApplyEventEnvelope[AggIdType, Event, EvtMeta](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateIdType", visible = true) aggregateId: AggIdType,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "eventType", visible = true) event: Event,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "metaEventType", visible = true) meta: EvtMeta) extends RoutableMessage[AggIdType]

  case class StateResponse[Agg](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateType", visible = true) aggregateState: Option[Agg])

  sealed trait CommandResponse
  case class CommandFailure(validationError: Seq[ValidationError]) extends CommandResponse
  case class CommandError(exception: Throwable) extends CommandResponse

  implicit def commandSuccessFormat[Agg](implicit format: Format[Agg]): Format[CommandSuccess[Agg]] = Json.format[CommandSuccess[Agg]]
  case class CommandSuccess[Agg](aggregateState: Option[Agg]) extends CommandResponse

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
 * @tparam AggId Aggregate id type
 * @tparam Agg Type of aggregate represented by this actor
 * @tparam Command Type of command handled by the aggregate
 * @tparam Event Type of events emitted by the aggregate
 * @tparam CmdMeta Type of metadata associated with incoming commands passed to the business logic to enhance commands
 * @tparam EvtMeta Type of metadata about events passed to the business logic to enhance published events
 */
private[core] class GenericAggregateActor[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    aggregateId: AggId,
    kafkaProducerActor: KafkaProducerActor[AggId, Agg, Event, EvtMeta],
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    metrics: GenericAggregateActor.GenericAggregateActorMetrics,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue]) extends Actor with Stash {
  import GenericAggregateActor._
  import context.dispatcher

  private case class InitializeWithState(stateOpt: Option[Agg])
  private case class EventsSuccessfullyPersisted(newState: InternalActorState, commandReceivedTime: Instant)

  private case class InternalActorState(stateOpt: Option[Agg])

  private val receiveTimeout = TimeoutConfig.AggregateActor.idleTimeout

  private val log = LoggerFactory.getLogger(getClass)

  override def preStart(): Unit = {
    initializeState(initializationAttempts = 0)
    super.preStart()
  }

  override def receive: Receive = uninitialized

  private def freeToProcess(state: InternalActorState): Receive = {
    case msg: CommandEnvelope[AggId, Command, CmdMeta] ⇒ handle(state, msg)
    case msg: ApplyEventEnvelope[AggId, Event, EvtMeta] ⇒ handle(state, msg)
    case _: GetState[AggId] ⇒ sender() ! state.stateOpt
    case ReceiveTimeout ⇒ handlePassivate()
    case Stop ⇒ handleStop()
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

  private def handle(state: InternalActorState, commandEnvelope: CommandEnvelope[AggId, Command, CmdMeta]): Unit = {
    val startTime = Instant.now
    context.setReceiveTimeout(Duration.Inf)
    context.become(persistingEvents(state))

    val futureEventsPersisted = processCommand(state, commandEnvelope).flatMap {
      case Right(Success(events)) ⇒
        val evtMeta = businessLogic.model.cmdMetaToEvtMeta(commandEnvelope.meta)
        val newState = events.foldLeft(state.stateOpt) { (state, evt) ⇒
          metrics.eventHandlingTimer.time(businessLogic.model.handleEvent(state, evt, evtMeta))
        }
        val eventKeyMetaValues = events.map(evt ⇒ (businessLogic.kafka.eventKeyExtractor(evt), evtMeta, evt))

        val stateKeyValue = aggregateId.toString -> newState

        log.trace("GenericAggregateActor for {} publishing messages", aggregateId)
        val publishFuture = if (events.nonEmpty) {
          kafkaProducerActor.publish(aggregateId = aggregateId, state = stateKeyValue, events = eventKeyMetaValues)
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

  private def handle(state: InternalActorState, applyEventEnvelope: ApplyEventEnvelope[AggId, Event, EvtMeta]): Unit = {
    val startTime = Instant.now
    context.setReceiveTimeout(Duration.Inf)
    context.become(persistingEvents(state))

    val newState = businessLogic.model.handleEvent(state.stateOpt, applyEventEnvelope.event, applyEventEnvelope.meta)
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
    commandEnvelope: CommandEnvelope[AggId, Command, CmdMeta]): Future[Either[Seq[ValidationError], Try[Seq[Event]]]] = {
    val commandPlusState = MessagePlusCurrentAggregate(commandEnvelope.command, state.stateOpt)

    import ValidationDSL._
    (commandPlusState mustSatisfy businessLogic.commandValidator).map { res ⇒
      res.map { _ ⇒
        metrics.commandHandlingTimer.time(
          businessLogic.model.processCommand(state.stateOpt, commandEnvelope.command, commandEnvelope.meta))
      }
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
      log.error(s"Could not initialize actor for ${aggregateId} after $initializationAttempts attempts.  Stopping actor")
      context.stop(self)
    } else {
      kafkaProducerActor.isAggregateStateCurrent(aggregateId.toString).map { isStateCurrent ⇒
        if (isStateCurrent) {
          fetchState(initializationAttempts)
        } else {
          val retryIn = RetryConfig.AggregateActor.initializeStateInterval
          log.warn(s"State for {} is not up to date in Kafka streams, retrying initialization in {}", aggregateId, retryIn)
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

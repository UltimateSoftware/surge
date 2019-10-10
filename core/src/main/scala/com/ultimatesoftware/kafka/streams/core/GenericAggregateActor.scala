// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, Props, ReceiveTimeout, Stash }
import akka.pattern.pipe
import com.typesafe.config.{ Config, ConfigFactory }
import com.ultimatesoftware.akka.cluster.Passivate
import com.ultimatesoftware.kafka.streams.AggregateStateStoreKafkaStreams
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, Timer }
import com.ultimatesoftware.scala.core.validations._
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.duration._

private[streams] object GenericAggregateActor {
  def props[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    aggregateId: AggId,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, _, CmdMeta],
    kafkaProducerActor: KafkaProducerActor[AggId, Agg, Event],
    metrics: GenericAggregateActorMetrics,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue]): Props = {
    Props(new GenericAggregateActor(aggregateId, kafkaProducerActor, businessLogic, metrics, kafkaStreamsCommand))
      .withDispatcher("generic-aggregate-actor-dispatcher")
  }

  sealed trait RoutableMessage[AggId] {
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
  case class CommandEnvelope[AggIdType, Cmd, CmdMeta](aggregateId: AggIdType, meta: CmdMeta, command: Cmd) extends RoutableMessage[AggIdType]
  case class GetState[AggIdType](aggregateId: AggIdType) extends RoutableMessage[AggIdType]

  case class StateResponse[Agg](aggregateState: Option[Agg])

  sealed trait CommandResponse
  case class CommandFailure(validationError: Seq[ValidationError])

  implicit def commandSuccessFormat[Agg](implicit format: Format[Agg]): Format[CommandSuccess[Agg]] = Json.format[CommandSuccess[Agg]]
  case class CommandSuccess[Agg](aggregateState: Option[Agg])

  def createMetrics(metricsProvider: MetricsProvider, aggregateName: String): GenericAggregateActorMetrics = {
    GenericAggregateActorMetrics(
      stateInitializationTimer = metricsProvider.createTimer(s"${aggregateName}ActorStateInitializationTimer"))
  }

  case class GenericAggregateActorMetrics(stateInitializationTimer: Timer)
  case object Stop
}

private[core] class GenericAggregateActor[AggId, Agg, Command, Event, CmdMeta, EvtMeta](
    aggregateId: AggId,
    kafkaProducerActor: KafkaProducerActor[AggId, Agg, Event],
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta],
    metrics: GenericAggregateActor.GenericAggregateActorMetrics,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue]) extends Actor with Stash {
  import GenericAggregateActor._
  import context.dispatcher

  private case class InitializeWithState(stateOpt: Option[Agg])
  private case class EventsSuccessfullyPersisted(newState: InternalActorState, commandReceivedTime: Instant)

  private case class InternalActorState(stateOpt: Option[Agg])

  private val maxInitializationAttempts = 10

  private val config: Config = ConfigFactory.load()
  private val receiveTimeout = config.getDuration("ulti.aggregate-actor.idle-timeout", TimeUnit.MILLISECONDS).milliseconds

  private val log = LoggerFactory.getLogger(getClass)

  context.system.scheduler.scheduleOnce(0.seconds)(initializeState(initializationAttempts = 0))

  override def receive: Receive = uninitialized

  private def freeToProcess(state: InternalActorState): Receive = {
    case msg: CommandEnvelope[AggId, Command, CmdMeta] ⇒ handle(state, msg)
    case msg: GetState[AggId] ⇒ sender() ! state.stateOpt
    case ReceiveTimeout ⇒ handlePassivate()
    case Stop ⇒ handleStop()
  }

  private def persistingEvents(state: InternalActorState): Receive = {
    case msg: EventsSuccessfullyPersisted ⇒ handle(state, msg)
    case msg: CommandFailure              ⇒ handle(state, msg)
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
      case Right(events) ⇒
        val evtMeta = businessLogic.model.cmdMetaToEvtMeta(commandEnvelope.meta)
        val newState = events.foldLeft(state.stateOpt) { (state, evt) ⇒ businessLogic.model.handleEvent(state, evt, evtMeta) }
        val eventKeyValues = events.map(evt ⇒ businessLogic.kafka.eventKeyExtractor(evt) -> evt)

        val states = newState.toSeq.flatMap(state ⇒ businessLogic.aggregateComposer.decompose(aggregateId, state).toSeq)
        val stateKeyValues = states.map(s ⇒ businessLogic.kafka.stateKeyExtractor(s) -> s)

        log.trace("GenericAggregateActor for {} sending events to publisher actor", aggregateId)
        kafkaProducerActor.publish(aggregateId = aggregateId, states = stateKeyValues, events = eventKeyValues).map { _ ⇒
          val newActorState = InternalActorState(stateOpt = newState)
          EventsSuccessfullyPersisted(newActorState, startTime)
        }
      case Left(validationErrors) ⇒
        Future.successful(CommandFailure(validationErrors))
    }

    futureEventsPersisted.recover {
      case ex ⇒
        // On an error, we don't know if the command succeeded or not, so crash the actor and force it to reinitialize
        log.error(s"Error while trying to persist events, crashing actor for ${businessLogic.aggregateName} $aggregateId", ex)
        throw ex
    }.pipeTo(self)(sender())
  }

  private def processCommand(
    state: InternalActorState,
    commandEnvelope: CommandEnvelope[AggId, Command, CmdMeta]): Future[Either[Seq[ValidationError], Seq[Event]]] = {
    val commandPlusState = MessagePlusCurrentAggregate(commandEnvelope.command, state.stateOpt)

    import ValidationDSL._
    (commandPlusState mustSatisfy businessLogic.commandValidator).map { res ⇒
      res.map { _ ⇒
        businessLogic.model.processCommand(state.stateOpt, commandEnvelope.command, commandEnvelope.meta)
          .toOption.getOrElse(Seq.empty)
      }
    }
  }

  private def handle(state: InternalActorState, failure: CommandFailure): Unit = {
    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(state))

    sender() ! failure
  }

  private def handle(state: InternalActorState, msg: EventsSuccessfullyPersisted): Unit = {
    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(msg.newState))

    val cmdSuccess = CommandSuccess(msg.newState.stateOpt)
    //log.trace(s"${businessLogic.aggregateName} $aggregateId returning response ${msg.newState.stateOpt.map(Json.toJson)}")

    sender() ! cmdSuccess

    unstashAll()
  }

  private def handle(initializeWithState: InitializeWithState): Unit = {
    log.debug(s"Actor for aggregate $aggregateId initializing with state ${initializeWithState.stateOpt}")
    unstashAll()

    val internalActorState = InternalActorState(
      stateOpt = initializeWithState.stateOpt)

    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(internalActorState))
  }

  private def initializeState(initializationAttempts: Int): Unit = {
    if (initializationAttempts > maxInitializationAttempts) {
      log.error(s"Could not initialize actor for ${aggregateId.toString} after $initializationAttempts attempts.  Stopping actor")
      context.stop(self)
    } else {
      kafkaProducerActor.isAggregateStateCurrent(aggregateId.toString).map { isStateCurrent ⇒
        if (isStateCurrent) {
          fetchState(initializationAttempts)
        } else {
          log.warn(s"State for {} is not up to date in Kafka streams, retrying initialization", aggregateId)
          context.system.scheduler.scheduleOnce(250.milliseconds)(initializeState(initializationAttempts + 1))
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
    val fetchedStateFut = metrics.stateInitializationTimer.time(
      kafkaStreamsCommand.substatesForAggregate(aggregateId.toString))

    fetchedStateFut.map { substates ⇒
      log.trace("GenericAggregateActor for {} fetched {} persisted substates", aggregateId, substates.length)
      val serializedStates = substates.map(_._2.state).toSet
      val stateOpt = businessLogic.aggregateComposer.compose(serializedStates)

      self ! InitializeWithState(stateOpt)
    }.recover {
      case _: NullPointerException ⇒
        log.error("Used null for aggregateId - this is weird. Will not try to initialize state again")
      case exception ⇒
        log.error(s"Failed to initialize ${businessLogic.aggregateName} actor for aggregate $aggregateId, retrying", exception)
        context.system.scheduler.scheduleOnce(3.seconds)(initializeState(initializationAttempts + 1))
    }
  }
}

// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, Props, ReceiveTimeout, Stash }
import akka.pattern.pipe
import com.typesafe.config.{ Config, ConfigFactory }
import com.ultimatesoftware.akka.cluster.Passivate
import com.ultimatesoftware.kafka.streams.AggregateStateStoreKafkaStreams
import com.ultimatesoftware.scala.core.domain._
import com.ultimatesoftware.scala.core.messaging.{ EventMessage, EventProperties, StateMessage }
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, Timer }
import com.ultimatesoftware.scala.core.validations._
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.duration._

private[streams] object GenericAggregateActor {
  def props[Agg, AggIdType, Command, Event, CmdMeta](
    aggregateId: AggIdType,
    businessLogic: DomainBusinessLogicAdapter[Agg, AggIdType, Command, Event, _, CmdMeta],
    kafkaProducerActor: KafkaProducerActor[Agg, AggIdType, Event],
    metrics: GenericAggregateActorMetrics,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams): Props = {
    Props(new GenericAggregateActor(aggregateId, kafkaProducerActor, businessLogic, metrics, kafkaStreamsCommand))
      .withDispatcher("generic-aggregate-actor-dispatcher")
  }

  sealed trait RoutableMessage[AggIdType] {
    def aggregateId: AggIdType
  }
  implicit def commandEnvelopeFormat[AggIdType, Cmd, CmdMeta](implicit
    aggIdTypeFormat: Format[AggIdType],
    cmdFormat: Format[Cmd],
    propsFormat: Format[CmdMeta]): Format[CommandEnvelope[AggIdType, Cmd, CmdMeta]] = {
    Json.format[CommandEnvelope[AggIdType, Cmd, CmdMeta]]
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

private[core] class GenericAggregateActor[Agg, AggIdType, Command, Event, CmdMeta](
    aggregateId: AggIdType,
    kafkaProducerActor: KafkaProducerActor[Agg, AggIdType, Event],
    businessLogic: DomainBusinessLogicAdapter[Agg, AggIdType, Command, Event, _, CmdMeta],
    metrics: GenericAggregateActor.GenericAggregateActorMetrics,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams) extends Actor with Stash {
  import context.dispatcher
  import GenericAggregateActor._

  private case class StateAccum(eventProps: EventProperties, stateOpt: Option[Agg])

  private case class InitializeWithState(stateOpt: Option[Agg], sequenceNumber: Option[Int], stateChecksum: Map[String, String])
  private case class EventsSuccessfullyPersisted(newState: InternalActorState, commandReceivedTime: Instant)

  private case class InternalActorState(stateOpt: Option[Agg], sequenceNumber: Int, stateChecksum: Map[String, String])

  private val maxInitializationAttempts = 10

  private val config: Config = ConfigFactory.load()
  private val receiveTimeout = config.getDuration("ulti.aggregate-actor.idle-timeout", TimeUnit.MILLISECONDS).milliseconds
  private val initialSequenceNumber = config.getInt("ulti.messaging.initial-sequence-number")

  private val log = LoggerFactory.getLogger(getClass)

  context.system.scheduler.scheduleOnce(0.seconds)(initializeState(initializationAttempts = 0))

  override def receive: Receive = uninitialized

  private def freeToProcess(state: InternalActorState): Receive = {
    case msg: CommandEnvelope[AggIdType, Command, CmdMeta] ⇒ handle(state, msg)
    case msg: GetState[AggIdType] ⇒ sender() ! state.stateOpt
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

  private def newStateAndEventMessages(baseStateAccum: StateAccum, events: Seq[Event]): (StateAccum, Seq[EventMessage[Event]]) = {
    events.foldLeft(baseStateAccum -> Seq.empty[EventMessage[Event]]) {
      case ((stateAccum, eventAccum), baseEvent) ⇒
        val eventMessage = EventMessage.create(stateAccum.eventProps, baseEvent, businessLogic.eventTypeInfo(baseEvent))
        val newEvents = eventAccum :+ eventMessage

        val newStateOpt: Option[Agg] = businessLogic.processEvent(stateAccum.stateOpt, eventMessage)
        val updatedProps = stateAccum.eventProps.withSequenceNumber(stateAccum.eventProps.sequenceNumber + 1)

        val newStateAccum = StateAccum(eventProps = updatedProps, stateOpt = newStateOpt)

        newStateAccum -> newEvents
    }
  }

  private def stateMessages(state: InternalActorState, currentStateAccum: StateAccum): Seq[StateMessage[JsValue]] = {
    currentStateAccum.stateOpt.map { newState ⇒
      businessLogic.aggregateComposer.decompose(aggregateId, newState).map { serialized ⇒
        StateMessage.create[JsValue](
          id = serialized.fullIdentifier,
          aggregateId = Some(aggregateId.toString),
          tenantId = currentStateAccum.eventProps.tenantId,
          state = Some(serialized.stateJson),
          eventSequenceNumber = currentStateAccum.eventProps.sequenceNumber - 1,
          prevStateChecksum = state.stateChecksum.get(serialized.fullIdentifier),
          typeInfo = serialized.typeInfo)
      }.toSeq
    }.getOrElse(Seq.empty)
  }

  private def handle(state: InternalActorState, commandEnvelope: CommandEnvelope[AggIdType, Command, CmdMeta]): Unit = {
    val startTime = Instant.now
    context.setReceiveTimeout(Duration.Inf)
    context.become(persistingEvents(state))

    val futureEventsPersisted = processCommand(state, commandEnvelope).flatMap {
      case Right(events) ⇒
        val baseStateAccum = StateAccum(
          eventProps = businessLogic.metaToPartialEventProps(commandEnvelope.meta).withSequenceNumber(state.sequenceNumber),
          stateOpt = state.stateOpt)

        val (currentStateAccum, eventMessages) = newStateAndEventMessages(baseStateAccum, events)

        val states = stateMessages(state, currentStateAccum)

        log.trace("GenericAggregateActor for {} sending events to publisher actor", aggregateId)
        kafkaProducerActor.publish(aggregateId = aggregateId, states = states, events = eventMessages).map { _ ⇒
          val newActorState = InternalActorState(
            stateOpt = currentStateAccum.stateOpt,
            sequenceNumber = currentStateAccum.eventProps.sequenceNumber,
            stateChecksum = states.flatMap(state ⇒ state.checksum.map(checksum ⇒ state.fullIdentifier -> checksum)).toMap)
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
    commandEnvelope: CommandEnvelope[AggIdType, Command, CmdMeta]): Future[Either[Seq[ValidationError], Seq[Event]]] = {
    val commandPlusState = MessagePlusCurrentAggregate(commandEnvelope.command, state.stateOpt)

    import ValidationDSL._
    (commandPlusState mustSatisfy businessLogic.commandValidator).map { res ⇒
      res.map { _ ⇒
        businessLogic.processCommand(state.stateOpt, commandEnvelope.command, commandEnvelope.meta)
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
      stateOpt = initializeWithState.stateOpt,
      sequenceNumber = initializeWithState.sequenceNumber.map(_ + 1).getOrElse(initialSequenceNumber),
      stateChecksum = initializeWithState.stateChecksum)

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
      val serializedStates = substates.map(_._2.state).flatMap(JsonStateRepr.fromStateMessage)
      val stateOpt = businessLogic.aggregateComposer.compose(serializedStates.toSet)

      val sequenceNumbers = substates.map(_._2.state.eventSequenceNumber)
      val sequenceNumber = if (sequenceNumbers.nonEmpty) {
        Some(sequenceNumbers.max)
      } else {
        None
      }
      if (substates.exists(tup ⇒ !sequenceNumber.contains(tup._2.state.eventSequenceNumber))) {
        log.error(s"Substates for aggregate $aggregateId contain different sequence numbers. This is weird and shouldn't happen. " +
          s"Using the highest sequence number found")
      }

      val checksums = substates.flatMap { tup ⇒
        val stateMsg = tup._2.state
        stateMsg.checksum.map { checksum ⇒
          stateMsg.fullIdentifier -> checksum
        }
      }.toMap

      self ! InitializeWithState(stateOpt, sequenceNumber, checksums)
    }.recover {
      case _: NullPointerException ⇒
        log.error("Used null for aggregateId - this is weird. Will not try to initialize state again")
      case exception ⇒
        log.error(s"Failed to initialize ${businessLogic.aggregateName} actor for aggregate $aggregateId, retrying", exception)
        context.system.scheduler.scheduleOnce(3.seconds)(initializeState(initializationAttempts + 1))
    }
  }
}

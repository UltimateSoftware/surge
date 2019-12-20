// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, Props, ReceiveTimeout, Stash }
import akka.pattern.pipe
import com.typesafe.config.{ Config, ConfigFactory }
import com.ultimatesoftware.akka.cluster.Passivate
import com.ultimatesoftware.kafka.streams.AggregateStateStoreKafkaStreams
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, Timer }
import com.ultimatesoftware.scala.core.utils.JsonFormats
import com.ultimatesoftware.scala.core.validations._
import com.ultimatesoftware.scala.oss.domain.AggregateSegment
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.duration._

private[streams] object GenericAggregateActor {
  def props[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope](
    aggregateId: AggId,
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, _, EvtMeta, Envelope],
    kafkaProducerActor: KafkaProducerActor[AggId, Agg, Event, EvtMeta, Envelope],
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
private[core] class GenericAggregateActor[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope](
    aggregateId: AggId,
    kafkaProducerActor: KafkaProducerActor[AggId, Agg, Event, EvtMeta, Envelope],
    businessLogic: KafkaStreamsCommandBusinessLogic[AggId, Agg, Command, Event, CmdMeta, EvtMeta, Envelope],
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
    case _: GetState[AggId] ⇒ sender() ! state.stateOpt
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
        val eventKeyMetaValues = events.map(evt ⇒ (businessLogic.kafka.eventKeyExtractor(evt), evtMeta, evt))

        val states = newState.toSeq.flatMap(state ⇒ businessLogic.aggregateComposer.decompose(aggregateId, state).toSeq)
        val stateKeyValues = states.map(s ⇒ businessLogic.kafka.stateKeyExtractor(s.value) -> s)

        log.trace("GenericAggregateActor for {} sending events to publisher actor", aggregateId)
        kafkaProducerActor.publish(aggregateId = aggregateId, states = stateKeyValues, events = eventKeyMetaValues).map { _ ⇒
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
    if (businessLogic.aggregateComposer.expectSingleEntity) {
      initFromSingleState(initializationAttempts)
    } else {
      initFromSubstates(initializationAttempts)
    }
  }

  private def initFromSingleState(initializationAttempts: Int): Unit = {
    val fetchedStateFut = metrics.stateInitializationTimer.time(
      kafkaStreamsCommand.aggregateQueryableStateStore.get(aggregateId.toString))

    fetchedStateFut.map { state ⇒
      val stateSet: Set[AggregateSegment[AggId, Agg]] = state.toSeq.toSet[Array[Byte]].map(s ⇒ {
        val stateOpt = businessLogic.readFormatting.readState(s)

        val objJson = JsonFormats.genericJacksonMapper.writer().writeValueAsString(stateOpt.get)
        AggregateSegment[AggId, Agg](
          aggregateId.toString,
          Json.parse(objJson), Some(stateOpt.get.getClass))
      })
      log.trace("GenericAggregateActor for {} fetched {} persisted substate", aggregateId, stateSet.size)
      val stateOpt = businessLogic.aggregateComposer.compose(stateSet)

      self ! InitializeWithState(stateOpt)
    } recover {
      case _: NullPointerException ⇒
        log.error("Used null for aggregateId - this is weird. Will not try to initialize state again")
      case exception ⇒
        log.error(s"Failed to initialize ${businessLogic.aggregateName} actor for aggregate $aggregateId, retrying", exception)
        context.system.scheduler.scheduleOnce(3.seconds)(initializeState(initializationAttempts + 1))
    }
  }

  private def initFromSubstates(initializationAttempts: Int): Unit = {
    val fetchedStateFut = metrics.stateInitializationTimer.time(
      kafkaStreamsCommand.substatesForAggregate(aggregateId.toString))

    fetchedStateFut.map { substates ⇒
      log.trace("GenericAggregateActor for {} fetched {} persisted substates", aggregateId, substates.length)
      val serializedStates = substates.map(_._2).toSet[Array[Byte]].map(s ⇒ {
        val stateOpt: Option[Agg] = businessLogic.readFormatting.readState(s)
        val objJson = JsonFormats.genericJacksonMapper.writer().writeValueAsString(stateOpt.get)

        AggregateSegment[AggId, Agg](
          aggregateId.toString,
          Json.parse(objJson), Some(stateOpt.get.getClass))
      })
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

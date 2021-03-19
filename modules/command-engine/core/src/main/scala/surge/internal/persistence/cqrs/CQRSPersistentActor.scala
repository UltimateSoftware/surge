// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence.cqrs

import java.time.Instant
import java.util.concurrent.Executors

import akka.actor.{ NoSerializationVerificationNeeded, Props, ReceiveTimeout, Stash }
import akka.pattern._
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.typesafe.config.ConfigFactory
import io.opentracing.{ Span, Tracer }
import org.slf4j.LoggerFactory
import surge.akka.cluster.{ JacksonSerializable, Passivate }
import surge.core._
import surge.internal.akka.ActorWithTracing
import surge.internal.config.{ RetryConfig, TimeoutConfig }
import surge.internal.kafka.HeadersHelper
import surge.internal.utils.SpanExtensions._
import surge.kafka.streams.AggregateStateStoreKafkaStreams
import surge.metrics.{ MetricInfo, Metrics, Timer }

import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

private[surge] object CQRSPersistentActor {
  private val config = ConfigFactory.load()
  private val serializationThreadPoolSize = config.getInt("surge.serialization.thread-pool-size")
  val serializationExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(serializationThreadPoolSize))

  def props[Agg, Command, Event](
    aggregateId: String,
    businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event],
    regionSharedResources: SurgeCQRSPersistentEntitySharedResources): Props = {
    Props(new CQRSPersistentActor(aggregateId, businessLogic, regionSharedResources))
      .withDispatcher("generic-aggregate-actor-dispatcher")
  }

  sealed trait CQRSPersistentActorCommand extends RoutableMessage
  case class CommandEnvelope[Cmd](
      aggregateId: String,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "cmdType", visible = true) command: Cmd) extends CQRSPersistentActorCommand
  case class GetState(aggregateId: String) extends CQRSPersistentActorCommand
  case class ApplyEventEnvelope[Event](
      aggregateId: String,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "eventType", visible = true) event: Event) extends CQRSPersistentActorCommand

  case class StateResponse[Agg](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateType", visible = true) aggregateState: Option[Agg])
    extends JacksonSerializable

  sealed trait CommandResponse extends JacksonSerializable
  case class CommandSuccess[Agg](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateType", visible = true) aggregateState: Option[Agg]) extends CommandResponse
  case class CommandError(exception: Throwable) extends CommandResponse with NoSerializationVerificationNeeded

  def createMetrics(metrics: Metrics, aggregateName: String): CQRSPersistentActorMetrics = {
    CQRSPersistentActorMetrics(
      stateInitializationTimer = metrics.timer(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.actor-state-initialization-timer",
          description = "Average time in milliseconds taken to load aggregate state from the KTable",
          tags = Map("aggregate" -> aggregateName))),
      aggregateDeserializationTimer = metrics.timer(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.aggregate-state-deserialization-timer",
          description = "Average time taken in milliseconds to deserialize aggregate state after the bytes are read from the KTable",
          tags = Map("aggregate" -> aggregateName))),
      commandHandlingTimer = metrics.timer(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.command-handling-timer",
          description = "Average time taken in milliseconds for the business logic 'processCommand' function to process a command",
          tags = Map("aggregate" -> aggregateName))),
      eventHandlingTimer = metrics.timer(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.event-handling-timer",
          description = "Average time taken in milliseconds for the business logic 'handleEvent' function to handle an event",
          tags = Map("aggregate" -> aggregateName))),
      serializeStateTimer = metrics.timer(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.aggregate-state-serialization-timer",
          description = "Average time taken in milliseconds to serialize a new aggregate state to bytes before persisting to Kafka",
          tags = Map("aggregate" -> aggregateName))),
      serializeEventTimer = metrics.timer(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.event-serialization-timer",
          description = "Average time taken in milliseconds to serialize an individual event to bytes before persisting to Kafka",
          tags = Map("aggregate" -> aggregateName))),
      eventPublishTimer = metrics.timer(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.event-publish-timer",
          description = "Average time taken in milliseconds to persist all generated events plus an updated state to Kafka",
          tags = Map("aggregate" -> aggregateName))))
  }

  case class CQRSPersistentActorMetrics(
      stateInitializationTimer: Timer,
      aggregateDeserializationTimer: Timer,
      commandHandlingTimer: Timer,
      eventHandlingTimer: Timer,
      serializeStateTimer: Timer,
      serializeEventTimer: Timer,
      eventPublishTimer: Timer) extends KTableInitializationMetrics with KTablePersistenceMetrics
  case object Stop
}

trait CQRSPersistentActorSupport[Agg, Command, Event] extends KTableInitializationSupport[Agg] with KTablePersistenceSupport[Agg, Event] {
  def businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event]
  def regionSharedResources: SurgeCQRSPersistentEntitySharedResources

  override def aggregateName: String = businessLogic.aggregateName
  override def initializationMetrics: KTableInitializationMetrics = regionSharedResources.metrics
  override def ktablePersistenceMetrics: KTablePersistenceMetrics = regionSharedResources.metrics
  override def kafkaProducerActor: KafkaProducerActor = regionSharedResources.kafkaProducerActor
  override def kafkaStreamsCommand: AggregateStateStoreKafkaStreams[_] = regionSharedResources.kafkaStreamsCommand
  override def deserializeState(bytes: Array[Byte]): Option[Agg] = businessLogic.readFormatting.readState(bytes)
}

/**
 * The CQRSPersistentActor represents a business logic aggregate.  It handles commands
 * using the given business logic and emits events and state to Kafka.  When run as part
 * of the Surge command engine, there is exactly one CQRSPersistentActor responsible for
 * each aggregate.
 *
 * @param aggregateId The aggregate id for the aggregate represented by this actor
 * @param businessLogic Domain business logic used to determine the events to emit and how to update
 *                      state based on new events.
 * @tparam Agg Type of aggregate represented by this actor
 * @tparam Command Type of command handled by the aggregate
 * @tparam Event Type of events emitted by the aggregate
 */
private[surge] class CQRSPersistentActor[Agg, Command, Event](
    override val aggregateId: String,
    override val businessLogic: SurgeCommandBusinessLogic[Agg, Command, Event],
    override val regionSharedResources: SurgeCQRSPersistentEntitySharedResources) extends ActorWithTracing with Stash
  with CQRSPersistentActorSupport[Agg, Command, Event] {

  import CQRSPersistentActor._
  import context.dispatcher
  import regionSharedResources._

  private sealed trait Internal extends NoSerializationVerificationNeeded
  private case class InitializeWithState(stateOpt: Option[Agg]) extends Internal
  private case class PersistenceSuccess(newState: InternalActorState, startTime: Instant) extends Internal
  private case class PersistenceFailure(newState: InternalActorState, reason: Throwable, numberOfFailures: Int,
      serializedEvents: Seq[KafkaProducerActor.MessageToPublish],
      serializedState: KafkaProducerActor.MessageToPublish, startTime: Instant) extends Internal
  private case class EventPublishTimedOut(reason: Throwable, startTime: Instant) extends Internal

  protected case class InternalSpans(commandProcessingSpan: Option[Span] = None)
  protected case class InternalActorState(stateOpt: Option[Agg], spans: InternalSpans = InternalSpans()) {
    def withCommandProcessingSpan(span: Span): InternalActorState = copy(spans = spans.copy(commandProcessingSpan = Some(span)))
    def completeCommandProcessingSpan(): Unit = spans.commandProcessingSpan.foreach(_.finish())
  }
  override type ActorState = InternalActorState

  private val receiveTimeout = TimeoutConfig.AggregateActor.idleTimeout

  private val log = LoggerFactory.getLogger(getClass)
  private val publishStateOnly: Boolean = businessLogic.kafka.publishStateOnly

  override def preStart(): Unit = {
    initializeState(initializationAttempts = 0, None)
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

  override def receiveWhilePersistingEvents(state: InternalActorState): Receive = {
    case msg: CommandError => handleCommandError(state, msg)
    case ReceiveTimeout    => // Ignore and drop ReceiveTimeout messages from this state
    case other =>
      log.info(s"Aggregate actor for $aggregateId stashing a message with class [{}] from the 'persistingEvents' state", other.getClass)
      stash()
  }

  private def initializationFailed(error: CommandError): Receive = {
    case _: CommandEnvelope[Command]  => sender() ! error
    case _: ApplyEventEnvelope[Event] => sender() ! error
    case Stop                         => handleStop()
  }

  def onInitializationFailed(cause: Throwable): Unit = {
    log.error(s"Could not initialize actor for $aggregateId after ${RetryConfig.AggregateActor.maxInitializationAttempts} attempts.  Stopping actor")
    context.become(initializationFailed(CommandError(cause)))
    unstashAll() // Handle any pending messages before stopping so we can reply with an explicit error instead of timing out
    self ! Stop
  }
  override def onInitializationSuccess(model: Option[Agg]): Unit = {
    self ! InitializeWithState(model)
  }

  override def onPersistenceSuccess(newState: InternalActorState): Unit = {
    newState.completeCommandProcessingSpan()
    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(newState))

    val cmdSuccess = CommandSuccess(newState.stateOpt)
    sender() ! cmdSuccess
    unstashAll()
  }

  override def onPersistenceFailure(state: InternalActorState, cause: Throwable): Unit = {
    state.spans.commandProcessingSpan.foreach(_.error(cause))
    state.completeCommandProcessingSpan()
    log.error(s"Error while trying to publish to Kafka, crashing actor for $aggregateName $aggregateId", cause)
    sender() ! CommandError(cause)
    context.stop(self)
  }

  override val tracer: Tracer = businessLogic.tracer

  private def handle(state: InternalActorState, commandEnvelope: CommandEnvelope[Command]): Unit = {
    val handleCommandSpan = createSpan("process_command").setTag("aggregateId", aggregateId)
    context.setReceiveTimeout(Duration.Inf)
    context.become(persistingEvents(state.withCommandProcessingSpan(handleCommandSpan)))

    val futureEventsPersisted = processCommand(state, commandEnvelope) match {
      case Success(events) =>
        handleEvents(state, events) match {
          case Success(newState) =>
            val serializedStateFut = serializeState(newState)

            val serializedEventsFut = if (publishStateOnly) {
              Future.successful(Seq.empty)
            } else {
              serializeEvents(events)
            }

            for {
              serializedEvents <- serializedEventsFut
              serializedState <- serializedStateFut
              publishResult <- doPublish(state.copy(stateOpt = newState), serializedEvents, serializedState,
                startTime = Instant.now, didStateChange = state.stateOpt != newState)
            } yield {
              publishResult
            }
          case Failure(exception) =>
            Future.successful(CommandError(exception))
        }
      case Failure(exception) =>
        Future.successful(CommandError(exception))
    }

    futureEventsPersisted.pipeTo(self)(sender())
  }

  private def handle(state: InternalActorState, applyEventEnvelope: ApplyEventEnvelope[Event]): Unit = {
    handleEvents(state, Seq(applyEventEnvelope.event)) match {
      case Success(newState) =>
        context.setReceiveTimeout(Duration.Inf)
        context.become(persistingEvents(state))
        val futureStatePersisted = serializeState(newState).flatMap { serializedState =>
          doPublish(state.copy(stateOpt = newState), serializedEvents = Seq.empty,
            serializedState = serializedState, startTime = Instant.now, didStateChange = state.stateOpt != newState)
        }
        futureStatePersisted.pipeTo(self)(sender())
      case Failure(e) =>
        sender() ! CommandError(e)
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
  }(CQRSPersistentActor.serializationExecutionContext)

  private def serializeState(stateValueOpt: Option[Agg]): Future[KafkaProducerActor.MessageToPublish] = Future {
    val serializedStateOpt = stateValueOpt.map { value =>
      metrics.serializeStateTimer.time(businessLogic.writeFormatting.writeState(value))
    }
    val stateValue = serializedStateOpt.map(_.value).orNull
    val stateHeaders = serializedStateOpt.map(ser => HeadersHelper.createHeaders(ser.headers)).orNull
    KafkaProducerActor.MessageToPublish(aggregateId.toString, stateValue, stateHeaders)
  }(CQRSPersistentActor.serializationExecutionContext)

  private def processCommand(state: InternalActorState, commandEnvelope: CommandEnvelope[Command]): Try[Seq[Event]] = {
    metrics.commandHandlingTimer.time(businessLogic.model.processCommand(state.stateOpt, commandEnvelope.command))
  }

  private def handleEvents(state: InternalActorState, events: Seq[Event]): Try[Option[Agg]] = {
    Try(events.foldLeft(state.stateOpt) { (state, evt) =>
      metrics.eventHandlingTimer.time(businessLogic.model.handleEvent(state, evt))
    })
  }

  private def handleCommandError(state: InternalActorState, error: CommandError): Unit = {
    log.debug(s"The command for ${businessLogic.aggregateName} $aggregateId resulted in an error", error.exception)
    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(state))

    sender() ! error
  }

  private def handle(initializeWithState: InitializeWithState): Unit = {
    log.debug(s"Actor state for aggregate $aggregateId successfully initialized")
    unstashAll()

    val internalActorState = InternalActorState(
      stateOpt = initializeWithState.stateOpt)

    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(internalActorState))
  }

  private def handlePassivate(): Unit = {
    log.trace(s"CQRSPersistentActor for aggregate ${businessLogic.aggregateName} $aggregateId is passivating gracefully")
    context.parent ! Passivate(Stop)
  }

  private def handleStop(): Unit = {
    log.trace(s"CQRSPersistentActor for aggregate ${businessLogic.aggregateName} $aggregateId is stopping gracefully")
    context.stop(self)
  }
}

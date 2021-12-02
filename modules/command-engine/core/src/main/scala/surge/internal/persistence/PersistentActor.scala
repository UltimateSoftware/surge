// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import akka.actor.{ Actor, ActorContext, NoSerializationVerificationNeeded, Props, ReceiveTimeout, Stash, Status }
import akka.pattern.pipe
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.typesafe.config.{ Config, ConfigFactory }
import io.opentelemetry.api.trace.Tracer
import org.slf4j.{ Logger, LoggerFactory }
import surge.akka.cluster.{ JacksonSerializable, Passivate }
import surge.core._
import surge.health.HealthSignalBusTrait
import surge.internal.SurgeModel
import surge.internal.akka.ActorWithTracing
import surge.internal.config.{ RetryConfig, TimeoutConfig }
import surge.internal.domain.{ Callback, SurgeContextImpl, SurgeSideEffect }
import surge.internal.kafka.HeadersHelper
import surge.internal.persistence.PersistentActor.{ ACKError, ApplyEvent, GetState, ProcessMessage, StateResponse, Stop }
import surge.internal.utils.DiagnosticContextFuturePropagation
import surge.kafka.streams.AggregateStateStoreKafkaStreams
import surge.metrics.{ MetricInfo, Metrics, Timer }

import java.time.Instant
import java.util.concurrent.Executors
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }

object PersistentActor {

  sealed trait ActorMessage

  sealed trait RoutableActorMessage extends ActorMessage with RoutableMessage

  case class ProcessMessage[M](
      aggregateId: String,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "msgType", visible = true) message: M)
      extends RoutableActorMessage

  case class GetState(aggregateId: String) extends RoutableActorMessage

  case class ApplyEvent[E](
      aggregateId: String,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "eventType", visible = true) event: E)
      extends RoutableActorMessage

  case class StateResponse[S](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateType", visible = true) aggregateState: Option[S])
      extends JacksonSerializable

  sealed trait ACK
      extends ActorMessage
      with JacksonSerializable

      // FIXME This is an explicit response type now
  case class ACKSuccess[S](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateType", visible = true) aggregateState: Option[S])
      extends ACK

  case class ACKError(exception: Throwable) extends ACK with NoSerializationVerificationNeeded

  case class ACKRejection[R](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "rejectionType", visible = true) rejection: R)
      extends ACK

  case object Stop extends ActorMessage with JacksonSerializable

  case class MetricsQuiver(
      stateInitializationTimer: Timer,
      aggregateDeserializationTimer: Timer,
      commandHandlingTimer: Timer,
      messageHandlingTimer: Timer,
      eventHandlingTimer: Timer,
      eventPublishTimer: Timer)
      extends KTableInitializationMetrics
      with KTablePersistenceMetrics

  private[internal] def createMetrics(metrics: Metrics, aggregateName: String): MetricsQuiver = {
    MetricsQuiver(
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
      messageHandlingTimer = metrics.timer(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.command-handling-timer",
          description = "Average time taken in milliseconds for the business logic 'processCommand' function to process a message",
          tags = Map("aggregate" -> aggregateName))),
      eventHandlingTimer = metrics.timer(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.event-handling-timer",
          description = "Average time taken in milliseconds for the business logic 'handleEvent' function to handle an event",
          tags = Map("aggregate" -> aggregateName))),
      eventPublishTimer = metrics.timer(
        MetricInfo(
          name = s"surge.${aggregateName.toLowerCase()}.event-publish-timer",
          description = "Average time taken in milliseconds to persist all generated events plus an updated state to Kafka",
          tags = Map("aggregate" -> aggregateName))))

  }

  def props[S, M, E](
      aggregateId: String,
      businessLogic: SurgeModel[S, M, E],
      signalBus: HealthSignalBusTrait,
      regionSharedResources: PersistentEntitySharedResources,
      config: Config): Props = {
    Props(new PersistentActor(aggregateId, businessLogic, regionSharedResources, config))
  }

  val serializationThreadPoolSize: Int = ConfigFactory.load().getInt("surge.serialization.thread-pool-size")
  val serializationExecutionContext: ExecutionContext = new DiagnosticContextFuturePropagation(
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(serializationThreadPoolSize)))

}

case class SurgeBehaviorContext[State](actorContext: ActorContext, initialState: State)

class PersistentActor[State, Message, Event, BehaviorState](
    aggregateId: String,
    businessLogic: SurgeModel[State, Message, Event],
    regionSharedResources: PersistentEntitySharedResources,
    config: Config,
    initialState: Option[State] => BehaviorState,
    behaviorCreator: SurgeBehaviorContext[BehaviorState] => Actor.Receive)
    extends ActorWithTracing
    with Stash
    with KTablePersistenceSupport[State, Event, BehaviorState]
    with KTableInitializationSupport[State] {

  import PersistentActor._
  import context.dispatcher

  private sealed trait Internal extends NoSerializationVerificationNeeded

  private case class InitializeWithState(stateOpt: Option[State]) extends Internal

  private case class EventPublishTimedOut(reason: Throwable, startTime: Instant) extends Internal

  override val initializationMetrics: KTableInitializationMetrics = regionSharedResources.metrics

  override val ktablePersistenceMetrics: KTablePersistenceMetrics = regionSharedResources.metrics

  override val kafkaProducerActor: KafkaProducerActor = regionSharedResources.kafkaProducerActor

  override val kafkaStreamsCommand: AggregateStateStoreKafkaStreams[_] = regionSharedResources.stateStore

  override def deserializeState(bytes: Array[Byte]): Option[State] = businessLogic.aggregateReadFormatting.readState(bytes)

  override def retryConfig: RetryConfig = new RetryConfig(config)

  override val tracer: Tracer = businessLogic.tracer

  override val aggregateName: String = businessLogic.aggregateName

  protected val receiveTimeout: FiniteDuration = TimeoutConfig.AggregateActor.idleTimeout

  override protected val maxProducerFailureRetries: Int = config.getInt("surge.aggregate-actor.publish-failure-max-retries")

  protected val log: Logger = LoggerFactory.getLogger(getClass)

  override def messageNameForTracedMessages: MessageNameExtractor = { case t: ProcessMessage[_] =>
    s"ProcessMessage[${t.message.getClass.getSimpleName}]"
  }

  override def preStart(): Unit = {
    initializeState(initializationAttempts = 0, None)
    super.preStart()
  }

  override def receive: Receive = uninitialized

  private def handle(initializeWithState: InitializeWithState): Unit = {
    log.debug(s"Actor state for aggregate $aggregateId successfully initialized")
    unstashAll()

    val nextState = behaviorCreator(SurgeBehaviorContext(context, initialState(initializeWithState.stateOpt)))

    context.setReceiveTimeout(receiveTimeout)
    context.become(nextState)
  }

  private def uninitialized: Receive = {
    case msg: InitializeWithState => handle(msg)
    case ReceiveTimeout           =>
      // Ignore and drop ReceiveTimeout messages from this state
      log.warn(
        s"Aggregate actor for $aggregateId received a ReceiveTimeout message in uninitialized state. " +
          "This should not happen and is likely a logic error. Dropping the ReceiveTimeout message.")
    case other =>
      log.debug(s"PersistentActor actor for $aggregateId stashing a message with class [{}] from the 'uninitialized' state", other.getClass)
      activeSpan.addEvent("stashed")
      stash()
  }

  private def handleStop(): Unit = {
    log.trace(s"PersistentActor for aggregate ${businessLogic.aggregateName} $aggregateId is stopping gracefully")
    context.stop(self)
  }

  private def handleCommandError(state: BehaviorState, error: ACKError): Unit = {
    log.debug(s"The command for ${businessLogic.aggregateName} $aggregateId resulted in an error", error.exception)
    context.setReceiveTimeout(receiveTimeout)
    context.become(behaviorCreator(SurgeBehaviorContext(context, state)))

    sender() ! error
  }

  override def receiveWhilePersistingEvents(state: BehaviorState): Receive = {
    case msg: ACKError  => handleCommandError(state, msg)
    case ReceiveTimeout => // Ignore and drop ReceiveTimeout messages from this state
    case Status.Failure(e) =>
      log.error(s"Aggregate actor for $aggregateId saw an unexpected exception from the 'persistingEvents' state", e)
      self.forward(ACKError(e))
    case other =>
      log.info(s"Aggregate actor for $aggregateId stashing a message with class [{}] from the 'persistingEvents' state", other.getClass)
      stash()
  }

  private def initializationFailed(error: ACKError): Receive = {
    case _: ProcessMessage[Message] => sender() ! error
    case _: ApplyEvent[Event]       => sender() ! error
    case Stop                       => handleStop()
  }

  def onInitializationFailed(cause: Throwable): Unit = {
    log.error(s"Could not initialize actor for $aggregateId after ${retryConfig.AggregateActor.maxInitializationAttempts} attempts. Stopping actor", cause)
    context.become(initializationFailed(ACKError(cause)))
    unstashAll() // Handle any pending messages before stopping so we can reply with an explicit error instead of timing out
    self ! Stop
  }

  override def onInitializationSuccess(model: Option[State]): Unit = {
    self ! InitializeWithState(model)
  }

  override def onPersistenceSuccess(newState: BehaviorState, surgeContext: SurgeContextImpl[State, Event]): Unit = {
    activeSpan.log("Successfully persisted events + state")
    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(newState))

    surgeContext.sideEffects.foreach(effect => applySideEffect(newState.stateOpt, effect))

    unstashAll()
  }

  private def applySideEffect(state: Option[State], effect: SurgeSideEffect[State]): Unit = {
    effect match {
      case callback: Callback[State] => callback.sideEffect(state)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported side effect detected [${effect.getClass.getName}]")
    }
  }

  override def onPersistenceFailure(state: BehaviorState, cause: Throwable): Unit = {
    log.error(s"Error while trying to publish to Kafka, crashing actor for $aggregateName $aggregateId", cause)
    activeSpan.log("Failed to persist events + state")
    activeSpan.error(cause)
    sender() ! ACKError(cause)
    context.stop(self)
  }
}

// FIXME This should live somewhere else
case class CQRSPersistenceBehaviorActorState[State](stateOpt: Option[State])

class CQRSPersistenceBehavior[State, Message, Event](
    aggregateId: String,
    businessLogic: SurgeModel[State, Message, Event],
    regionSharedResources: PersistentEntitySharedResources,
    becomePersistingEvents: CQRSPersistenceBehaviorActorState[State] => Actor.Receive,
    persistenceSupport: KTablePersistenceSupport[State, Event, CQRSPersistenceBehaviorActorState[State]],
    context: ActorContext) {
  private val log = LoggerFactory.getLogger(getClass)
  import regionSharedResources._

  private val publishStateOnly: Boolean = businessLogic.kafka.eventsTopicOpt.isEmpty
  assert(publishStateOnly || businessLogic.eventWriteFormattingOpt.nonEmpty, "businessLogic.eventWriteFormattingOpt may not be none when publishing events")

  import context._
  def statefulReceive(state: CQRSPersistenceBehaviorActorState[State]): Actor.Receive = {
    case pm: ProcessMessage[Message] =>
      handle(state, pm)
    case ae: ApplyEvent[Event] =>
      doApplyEvent(state, ae)
    case GetState(_)    => sender() ! StateResponse(state.stateOpt)
    case ReceiveTimeout => handlePassivate()
    case Stop           => handleStop()
  }

  def handle(state: CQRSPersistenceBehaviorActorState[State], msg: ProcessMessage[Message]): Unit = {
    context.setReceiveTimeout(Duration.Inf)
    context.become(becomePersistingEvents(state))

    processMessage(state, msg)
      .flatMap { result =>
        if (result.isRejected) {
          // FIXME Temporary no-op publish to trigger side effects later
          persistenceSupport.doPublish(
            state,
            result,
            Seq.empty,
            KafkaProducerActor.MessageToPublish("", "".getBytes(), HeadersHelper.createHeaders(Map.empty)),
            startTime = Instant.now,
            didStateChange = false)
        } else {
          val serializingFut = if (publishStateOnly) {
            Future.successful(Seq.empty)
          } else {
            businessLogic.serializeEvents(result.events)
          }
          for {
            serializedState <- businessLogic.serializeState(aggregateId, result.state)
            serializedEvents <- serializingFut
            publishResult <- persistenceSupport.doPublish(
              state.copy(stateOpt = result.state),
              result,
              serializedEvents,
              serializedState,
              startTime = Instant.now,
              didStateChange = state.stateOpt != result.state)
          } yield {
            publishResult
          }
        }
      }
      .recover { case e =>
        ACKError(e)
      }
      .pipeTo(self)(sender())
  }

  def processMessage(state: CQRSPersistenceBehaviorActorState[State], ProcessMessage: ProcessMessage[Message]): Future[SurgeContextImpl[State, Event]] = {
    metrics.messageHandlingTimer
      .timeFuture {
        businessLogic.model.handle(SurgeContextImpl(sender()), state.stateOpt, ProcessMessage.message)
      }
      .mapTo[SurgeContextImpl[State, Event]]
  }

  def doApplyEvent(state: CQRSPersistenceBehaviorActorState[State], value: PersistentActor.ApplyEvent[Event]): Unit = {
    context.setReceiveTimeout(Duration.Inf)
    context.become(becomePersistingEvents(state))

    callEventHandler(state, value.event)
      .flatMap { context =>
        businessLogic.serializeState(aggregateId, context.state).flatMap { serializedState =>
          persistenceSupport.doPublish(
            state.copy(stateOpt = context.state),
            context,
            serializedEvents = Seq.empty,
            serializedState = serializedState,
            startTime = Instant.now,
            didStateChange = state.stateOpt != context.state)
        }
      }
      .recover { case e =>
        ACKError(e)
      }
      .pipeTo(self)(sender())
  }

  def callEventHandler(state: CQRSPersistenceBehaviorActorState[State], evt: Event): Future[SurgeContextImpl[State, Event]] = {
    metrics.eventHandlingTimer.timeFuture(businessLogic.model.applyAsync(SurgeContextImpl(sender()), state.stateOpt, evt)).mapTo[SurgeContextImpl[State, Event]]
  }

  def handlePassivate(): Unit = {
    log.trace(s"PersistentActor for aggregate ${businessLogic.aggregateName} $aggregateId is passivating gracefully")
    context.parent ! Passivate(Stop)
  }

  def handleStop(): Unit = {
    log.trace(s"PersistentActor for aggregate ${businessLogic.aggregateName} $aggregateId is stopping gracefully")
    context.stop(self)
  }
}

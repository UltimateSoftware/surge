// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import akka.actor.{ NoSerializationVerificationNeeded, Props, ReceiveTimeout, Stash, Status }
import akka.cluster.sharding.ShardRegion.Passivate
import akka.pattern.pipe
import com.fasterxml.jackson.annotation.{ JsonIgnoreProperties, JsonTypeInfo }
import com.typesafe.config.{ Config, ConfigFactory }
import io.opentelemetry.api.trace.Tracer
import org.slf4j.{ Logger, LoggerFactory }
import surge.akka.cluster.{ JacksonSerializable, Passivate => SurgePassivate }
import surge.core._
import surge.internal.SurgeModel
import surge.internal.akka.ActorWithTracing
import surge.internal.config.{ RetryConfig, TimeoutConfig }
import surge.internal.domain.{ Callback, SurgeContextImpl, SurgeSideEffect }
import surge.internal.tracing.RoutableMessage
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

  case class ApplyEvents[E](
      aggregateId: String,
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "eventType", visible = true) events: Seq[E])
      extends RoutableActorMessage

  case class StateResponse[S](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateType", visible = true) aggregateState: Option[S])
      extends JacksonSerializable

  sealed trait ACK extends ActorMessage with JacksonSerializable

  case class ACKSuccess[S](
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "aggregateType", visible = true) aggregateState: Option[S])
      extends ACK

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class ACKError(
      @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "exceptionType", visible = true) exception: Throwable)
      extends ACK

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
      stateInitializationTimer = metrics.timer(Metrics.SURGE_AGGREGATE_ACTOR_STATE_INITIALIZATION_TIMER.withTags(Map("aggregate" -> aggregateName))),
      aggregateDeserializationTimer = metrics.timer(Metrics.SURGE_AGGREGATE_STATE_DESERIALIZATION_TIMER.withTags(Map("aggregate" -> aggregateName))),
      commandHandlingTimer = metrics.timer(Metrics.SURGE_AGGREGATE_COMMAND_HANDLING_TIMER.withTags(Map("aggregate" -> aggregateName))),
      messageHandlingTimer = metrics.timer(Metrics.SURGE_AGGREGATE_MESSAGE_HANDLING_TIMER.withTags(Map("aggregate" -> aggregateName))),
      eventHandlingTimer = metrics.timer(Metrics.SURGE_AGGREGATE_EVENT_HANDLING_TIMER.withTags(Map("aggregate" -> aggregateName))),
      eventPublishTimer = metrics.timer(Metrics.SURGE_AGGREGATE_EVENT_PUBLISH_TIMER.withTags(Map("aggregate" -> aggregateName))))

  }

  def props[S, M, E](
      businessLogic: SurgeModel[S, M, E],
      regionSharedResources: PersistentEntitySharedResources,
      config: Config,
      aggregateIdOpt: Option[String] = None): Props = {
    Props(new PersistentActor(businessLogic, regionSharedResources, config, aggregateIdOpt))
  }

  val serializationThreadPoolSize: Int = ConfigFactory.load().getInt("surge.serialization.thread-pool-size")
  val serializationExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(serializationThreadPoolSize))
}

// scalastyle:off number.of.methods
class PersistentActor[S, M, E](
    val businessLogic: SurgeModel[S, M, E],
    val regionSharedResources: PersistentEntitySharedResources,
    config: Config,
    val aggregateIdOpt: Option[String])
    extends ActorWithTracing
    with Stash
    with KTablePersistenceSupport[S, E]
    with KTableInitializationSupport[S] {

  import PersistentActor._
  import context.dispatcher

  def aggregateId: String = aggregateIdOpt.getOrElse(self.path.name)

  private val metrics = regionSharedResources.metrics

  private val isAkkaClusterEnabled: Boolean = config.getBoolean("surge.feature-flags.experimental.enable-akka-cluster")

  private sealed trait Internal extends NoSerializationVerificationNeeded

  private case class InitializeWithState(stateOpt: Option[S]) extends Internal

  private case class PersistenceSuccess(newState: InternalActorState, startTime: Instant) extends Internal

  private case class PersistenceFailure(
      newState: InternalActorState,
      reason: Throwable,
      numberOfFailures: Int,
      serializedEvents: Seq[KafkaProducerActor.MessageToPublish],
      serializedState: KafkaProducerActor.MessageToPublish,
      startTime: Instant)
      extends Internal

  private case class EventPublishTimedOut(reason: Throwable, startTime: Instant, newState: ActorState) extends Internal

  protected case class InternalActorState(stateOpt: Option[S])

  override type ActorState = InternalActorState

  override val initializationMetrics: KTableInitializationMetrics = regionSharedResources.metrics

  override val ktablePersistenceMetrics: KTablePersistenceMetrics = regionSharedResources.metrics

  override val kafkaProducerActor: KafkaProducerActor = regionSharedResources.aggregateIdToKafkaProducer(aggregateId)

  override val kafkaStreamsCommand: AggregateStateStoreKafkaStreams = regionSharedResources.stateStore

  override def deserializeState(bytes: Array[Byte]): Option[S] = businessLogic.aggregateReadFormatting.readState(bytes)

  override def retryConfig: RetryConfig = new RetryConfig(config)

  override val tracer: Tracer = businessLogic.tracer

  override val aggregateName: String = businessLogic.aggregateName

  protected val receiveTimeout: FiniteDuration = TimeoutConfig.AggregateActor.idleTimeout

  override protected val maxProducerFailureRetries: Int = config.getInt("surge.aggregate-actor.publish-failure-max-retries")

  protected val log: Logger = LoggerFactory.getLogger(getClass)

  private val publishStateOnly: Boolean = businessLogic.kafka.eventsTopicOpt.isEmpty

  assert(publishStateOnly || businessLogic.eventWriteFormattingOpt.nonEmpty, "businessLogic.eventWriteFormattingOpt may not be none when publishing events")

  override def messageNameForTracedMessages: MessageNameExtractor = { case t: ProcessMessage[_] =>
    s"ProcessMessage[${t.message.getClass.getSimpleName}]"
  }

  override def preStart(): Unit = {
    initializeState(initializationAttempts = 0, None)
    super.preStart()
  }

  override def receive: Receive = uninitialized

  private def freeToProcess(state: InternalActorState): Receive = {
    case pm: ProcessMessage[M] =>
      handle(state, pm)
    case ae: ApplyEvents[E] =>
      doApplyEvent(state, ae)
    case GetState(_)    => sender() ! StateResponse(state.stateOpt)
    case ReceiveTimeout => handlePassivate()
    case Stop           => handleStop()
  }

  private def handle(initializeWithState: InitializeWithState): Unit = {
    log.debug(s"Actor state for aggregate $aggregateId successfully initialized")
    unstashAll()

    val internalActorState = InternalActorState(stateOpt = initializeWithState.stateOpt)

    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(internalActorState))
  }

  def handle(state: InternalActorState, msg: ProcessMessage[M]): Unit = {
    context.setReceiveTimeout(Duration.Inf)
    context.become(persistingEvents(state))

    processMessage(state, msg)
      .flatMap { result =>
        if (result.isRejected) {
          // FIXME Temporary no-op publish to trigger side effects later
          doPublish(state, result, Seq.empty, startTime = Instant.now, shouldPublish = false)
        } else {
          val serializingFut = if (publishStateOnly) {
            Future.successful(Seq.empty)
          } else {
            businessLogic.serializeEvents(result.events)
          }
          val isSomethingNewToPublish = result.events.nonEmpty || result.records.nonEmpty || (state.stateOpt != result.state)
          for {
            serializedState <- businessLogic.serializeState(aggregateId, result.state, kafkaProducerActor.assignedPartition)
            serializedEvents <- serializingFut
            additionalRecords = result.records.map(KafkaProducerActor.MessageToPublish)
            publishResult <- doPublish(
              state.copy(stateOpt = result.state),
              result,
              serializedEvents ++ additionalRecords :+ serializedState,
              startTime = Instant.now,
              shouldPublish = isSomethingNewToPublish)
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

  def processMessage(state: InternalActorState, ProcessMessage: ProcessMessage[M]): Future[SurgeContextImpl[S, E]] = {
    metrics.messageHandlingTimer
      .timeFuture {
        businessLogic.model.handle(
          SurgeContextImpl(sender(), defaultEventTopicOpt = businessLogic.kafka.eventsTopicOpt, state = state.stateOpt),
          state.stateOpt,
          ProcessMessage.message)
      }
      .mapTo[SurgeContextImpl[S, E]]
  }

  def doApplyEvent(state: InternalActorState, value: PersistentActor.ApplyEvents[E]): Unit = {
    context.setReceiveTimeout(Duration.Inf)
    context.become(persistingEvents(state))

    callEventHandler(state, value.events)
      .flatMap { context =>
        businessLogic.serializeState(aggregateId, context.state, kafkaProducerActor.assignedPartition).flatMap { serializedState =>
          doPublish(
            state.copy(stateOpt = context.state),
            context,
            serializedMessages = Seq(serializedState),
            startTime = Instant.now,
            shouldPublish = state.stateOpt != context.state)
        }
      }
      .recover { case e =>
        ACKError(e)
      }
      .pipeTo(self)(sender())
  }

  def callEventHandler(state: InternalActorState, evt: Seq[E]): Future[SurgeContextImpl[S, E]] = {
    metrics.eventHandlingTimer
      .timeFuture(
        businessLogic.model
          .applyAsync(SurgeContextImpl(sender(), defaultEventTopicOpt = businessLogic.kafka.eventsTopicOpt, state = state.stateOpt), state.stateOpt, evt))
      .mapTo[SurgeContextImpl[S, E]]
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

  private def handlePassivate(): Unit = {
    log.trace(s"PersistentActor for aggregate ${businessLogic.aggregateName} $aggregateId is passivating gracefully")

    // FIXME: temporary fix to support switch between akka and existing shard allocation strategy
    if (isAkkaClusterEnabled) {
      context.parent ! Passivate(Stop)
    } else {
      context.parent ! SurgePassivate(Stop)
    }
  }

  private def handleStop(): Unit = {
    log.trace(s"PersistentActor for aggregate ${businessLogic.aggregateName} $aggregateId is stopping gracefully")
    context.stop(self)
  }

  private def handleCommandError(state: InternalActorState, error: ACKError): Unit = {
    log.debug(s"The command for ${businessLogic.aggregateName} $aggregateId resulted in an error", error.exception)
    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(state))

    sender() ! error
  }

  override def receiveWhilePersistingEvents(state: InternalActorState): Receive = {
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
    case _: ProcessMessage[M] => sender() ! error
    case _: ApplyEvents[E]    => sender() ! error
    case Stop                 => handleStop()
  }

  def onInitializationFailed(cause: Throwable): Unit = {
    log.error(s"Could not initialize actor for $aggregateId after ${retryConfig.AggregateActor.maxInitializationAttempts} attempts. Stopping actor", cause)
    context.become(initializationFailed(ACKError(cause)))
    unstashAll() // Handle any pending messages before stopping so we can reply with an explicit error instead of timing out
    self ! Stop
  }

  override def onInitializationSuccess(model: Option[S]): Unit = {
    self ! InitializeWithState(model)
  }

  override def onPersistenceSuccess(newState: InternalActorState, surgeContext: SurgeContextImpl[S, E]): Unit = {
    activeSpan.log("Successfully persisted events + state")
    context.setReceiveTimeout(receiveTimeout)
    context.become(freeToProcess(newState))

    surgeContext.sideEffects.foreach(effect => applySideEffect(newState.stateOpt, effect))

    unstashAll()
  }

  private def applySideEffect(state: Option[S], effect: SurgeSideEffect[S]): Unit = {
    effect match {
      case callback: Callback[S] => callback.sideEffect(state)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported side effect detected [${effect.getClass.getName}]")
    }
  }

  override def onPersistenceFailure(state: InternalActorState, cause: Throwable): Unit = {
    log.error(s"Error while trying to publish to Kafka, crashing actor for $aggregateName $aggregateId", cause)
    activeSpan.log("Failed to persist events + state")
    activeSpan.error(cause)
    sender() ! ACKError(cause)

    context.stop(self)
  }
}

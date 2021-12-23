// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import io.opentelemetry.api.OpenTelemetry
import org.apache.kafka.common.TopicPartition
import play.api.libs.json._
import surge.core.command.{ SurgeCommandKafkaConfig, SurgeCommandModel }
import surge.internal.domain.{ SurgeContext, SurgeProcessingModel }
import surge.internal.tracing.{ NoopTracerFactory, RoutableMessage }
import surge.kafka.{ KafkaTopic, PartitionStringUpToColon }
import surge.metrics.Metrics

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

object TestBoundedContext {
  case class State(aggregateId: String, count: Int, version: Int)
  implicit val stateFormat: Format[State] = Json.format

  sealed trait BaseTestCommand extends RoutableMessage {
    def aggregateId: String
    def expectedVersion: Int = 0
  }

  case class WrappedTestCommand(topicPartition: TopicPartition, cmd: BaseTestCommand)

  case class Increment(incrementAggregateId: String) extends BaseTestCommand {
    val aggregateId: String = incrementAggregateId
  }

  case class Decrement(decrementAggregateId: String) extends BaseTestCommand {
    val aggregateId: String = decrementAggregateId
  }

  case class DoNothing(aggregateId: String) extends BaseTestCommand
  case class CreateNoOpEvent(aggregateId: String) extends BaseTestCommand

  case class FailCommandProcessing(failProcessingId: String, errorMsg: String) extends BaseTestCommand {
    val aggregateId: String = failProcessingId
  }
  case class CreateExceptionThrowingEvent(aggregateId: String, errorMsg: String) extends BaseTestCommand
  case class CreateUnserializableEvent(aggregateId: String, errorMsg: String) extends BaseTestCommand

  implicit val countIncrementedFormat: Format[CountIncremented] = Json.format
  implicit val countDecrementedFormat: Format[CountDecremented] = Json.format
  implicit val noopFormat: Format[NoOpEvent] = Json.format
  implicit val unserializableFormat: Format[UnserializableEvent] = new Format[UnserializableEvent] {
    override def reads(json: JsValue): JsResult[UnserializableEvent] = JsError("UnserializableEvent should not be deserialized")
    override def writes(o: UnserializableEvent): JsValue = throw o.throwable
  }
  implicit val exceptionThrowingFormat: Format[ExceptionThrowingEvent] = new Format[ExceptionThrowingEvent] {
    override def writes(o: ExceptionThrowingEvent): JsValue = JsNull
    override def reads(json: JsValue): JsResult[ExceptionThrowingEvent] = JsError("Exception throwing event should never be serialized")
  }
  implicit val baseTestEventFormat: Format[BaseTestEvent] = Json.format[BaseTestEvent]
  sealed trait BaseTestEvent {
    def aggregateId: String
    def sequenceNumber: Int
    def eventName: String
  }

  case class CountIncremented(aggregateId: String, incrementBy: Int, sequenceNumber: Int) extends BaseTestEvent {
    val eventName: String = "countIncremented"
  }

  case class CountDecremented(aggregateId: String, decrementBy: Int, sequenceNumber: Int) extends BaseTestEvent {
    val eventName: String = "countDecremented"
  }

  case class NoOpEvent(aggregateId: String, sequenceNumber: Int) extends BaseTestEvent {
    val eventName: String = "no-op"
  }

  case class ExceptionThrowingEvent(aggregateId: String, sequenceNumber: Int, errorMsg: String) extends BaseTestEvent {
    val eventName: String = "exception-throwing"
  }

  case class UnserializableEvent(aggregateId: String, sequenceNumber: Int, throwable: Throwable) extends BaseTestEvent {
    val eventName: String = "unserializable"
  }
}

trait TestBoundedContext {
  import TestBoundedContext._
  implicit val countIncrementedFormat: Format[CountIncremented] = Json.format
  implicit val countDecrementedFormat: Format[CountDecremented] = Json.format

  trait BusinessLogicTrait extends SurgeProcessingModel[State, BaseTestCommand, BaseTestEvent] {

    override def applyAsync(
        ctx: SurgeContext[State, BaseTestEvent],
        state: Option[State],
        events: Seq[BaseTestEvent]): Future[SurgeContext[State, BaseTestEvent]] = {

      val newState = events.foldLeft(Try(ctx))((stateAccum, evt) => stateAccum.map(_.updateState(handleEvent(state, evt))))

      Future.fromTry(newState.map(_.reply(s => s)))
    }
    def handleEvent(agg: Option[State], evt: BaseTestEvent): Option[State] = {

      val current = agg.getOrElse(State(evt.aggregateId, 0, 0))

      val newState = evt match {
        case CountIncremented(_, incrementBy, sequenceNumber) =>
          current.copy(count = current.count + incrementBy, version = sequenceNumber)
        case CountDecremented(_, decrementBy, sequenceNumber) =>
          current.copy(count = current.count - decrementBy, version = sequenceNumber)
        case _: NoOpEvent                              => current
        case UnserializableEvent(_, sequenceNumber, _) => current.copy(version = sequenceNumber)
        case ExceptionThrowingEvent(_, _, errorMsg)    => throw new Exception(errorMsg)
      }
      Some(newState)
    }

    override def handle(ctx: SurgeContext[State, BaseTestEvent], agg: Option[State], cmd: BaseTestCommand)(
        implicit ec: ExecutionContext): Future[SurgeContext[State, BaseTestEvent]] = {
      val newSequenceNumber = agg.map(_.version).getOrElse(0) + 1

      val futureEvents = cmd match {
        case Increment(aggregateId)       => Future.successful(Seq(CountIncremented(aggregateId, incrementBy = 1, sequenceNumber = newSequenceNumber)))
        case Decrement(aggregateId)       => Future.successful(Seq(CountDecremented(aggregateId, decrementBy = 1, sequenceNumber = newSequenceNumber)))
        case CreateNoOpEvent(aggregateId) => Future.successful(Seq(NoOpEvent(aggregateId, newSequenceNumber)))
        case _: DoNothing                 => Future.successful(Seq.empty)
        case fail: FailCommandProcessing =>
          Future.failed(new Exception(fail.errorMsg))
        case CreateExceptionThrowingEvent(aggregateId, errorMsg) =>
          Future.successful(Seq(ExceptionThrowingEvent(aggregateId, newSequenceNumber, errorMsg)))
        case CreateUnserializableEvent(aggregateId, errorMsg) =>
          Future.successful(Seq(UnserializableEvent(aggregateId, newSequenceNumber, new Exception(errorMsg))))
        case _ =>
          throw new RuntimeException("Received unexpected message in command handler! This should not happen and indicates a bad test")
      }
      futureEvents.map { events =>
        val newState = events.foldLeft(agg)((s, e) => handleEvent(s, e))
        ctx.persistEvents(events).updateState(newState).reply(s => s)
      }
    }
  }

  object BusinessLogic extends BusinessLogicTrait

  private val kafkaConfig = SurgeCommandKafkaConfig(
    stateTopic = KafkaTopic("testStateTopic"),
    eventsTopic = KafkaTopic("testEventsTopic"),
    publishStateOnly = false,
    streamsApplicationId = "count-aggregate-consumer-group-name",
    clientId = "",
    transactionalIdPrefix = "test-transaction-id-prefix")

  val aggregateFormatting: SurgeAggregateFormatting[State] = new SurgeAggregateFormatting[State] {
    override def writeState(agg: State): SerializedAggregate = {
      SerializedAggregate(Json.toJson(agg).toString().getBytes())
    }

    override def readState(bytes: Array[Byte]): Option[State] = Json.parse(bytes).asOpt[State]
  }

  val eventWriter: SurgeEventWriteFormatting[BaseTestEvent] = (evt: BaseTestEvent) => {
    SerializedMessage(s"${evt.aggregateId}:${evt.sequenceNumber}", Json.toJson(evt).toString().getBytes())
  }

  val businessLogic: SurgeCommandModel[State, BaseTestCommand, BaseTestEvent] =
    command.SurgeCommandModel(
      aggregateName = "CountAggregate",
      kafka = kafkaConfig,
      model = BusinessLogic,
      aggregateReadFormatting = aggregateFormatting,
      aggregateWriteFormatting = aggregateFormatting,
      eventWriteFormatting = eventWriter,
      metrics = Metrics.globalMetricRegistry,
      openTelemetry = OpenTelemetry.noop(),
      partitioner = PartitionStringUpToColon.instance,
      tracer = NoopTracerFactory.create())
}

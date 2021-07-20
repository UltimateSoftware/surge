// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import play.api.libs.json._
import surge.core.command.{ SurgeCommandKafkaConfig, SurgeCommandModel }
import surge.internal.domain.CommandHandler
import surge.internal.persistence.Context
import surge.internal.tracing.NoopTracerFactory
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

import scala.concurrent.Future

object TestBoundedContext {
  case class State(aggregateId: String, count: Int, version: Int)
  implicit val stateFormat: Format[State] = Json.format

  sealed trait BaseTestCommand {
    def aggregateId: String
    def expectedVersion: Int = 0
  }

  case class Increment(incrementAggregateId: String) extends BaseTestCommand {
    val aggregateId: String = incrementAggregateId
  }

  case class Decrement(decrementAggregateId: String) extends BaseTestCommand {
    val aggregateId: String = decrementAggregateId
  }

  case class DoNothing(aggregateId: String) extends BaseTestCommand
  case class CreateNoOpEvent(aggregateId: String) extends BaseTestCommand

  case class FailCommandProcessing(failProcessingId: String, withError: Throwable) extends BaseTestCommand {
    val aggregateId: String = failProcessingId
  }
  case class CreateExceptionThrowingEvent(aggregateId: String, throwable: Throwable) extends BaseTestCommand
  case class CreateUnserializableEvent(aggregateId: String, throwable: Throwable) extends BaseTestCommand

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

  case class ExceptionThrowingEvent(aggregateId: String, sequenceNumber: Int, throwable: Throwable) extends BaseTestEvent {
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

  trait BusinessLogicTrait extends CommandHandler[State, BaseTestCommand, Nothing, BaseTestEvent] {

    override def apply(ctx: Context, agg: Option[State], evt: BaseTestEvent): Option[State] = handleEvent(agg, evt)
    def handleEvent(agg: Option[State], evt: BaseTestEvent): Option[State] = {

      val current = agg.getOrElse(State(evt.aggregateId, 0, 0))

      val newState = evt match {
        case CountIncremented(_, incrementBy, sequenceNumber) =>
          current.copy(count = current.count + incrementBy, version = sequenceNumber)
        case CountDecremented(_, decrementBy, sequenceNumber) =>
          current.copy(count = current.count - decrementBy, version = sequenceNumber)
        case _: NoOpEvent                              => current
        case UnserializableEvent(_, sequenceNumber, _) => current.copy(version = sequenceNumber)
        case ExceptionThrowingEvent(_, _, e)           => throw e
      }
      Some(newState)
    }

    override def processCommand(ctx: Context, agg: Option[State], cmd: BaseTestCommand): Future[CommandResult] = {
      val newSequenceNumber = agg.map(_.version).getOrElse(0) + 1

      cmd match {
        case Increment(aggregateId)       => Future.successful(Right(Seq(CountIncremented(aggregateId, incrementBy = 1, sequenceNumber = newSequenceNumber))))
        case Decrement(aggregateId)       => Future.successful(Right(Seq(CountDecremented(aggregateId, decrementBy = 1, sequenceNumber = newSequenceNumber))))
        case CreateNoOpEvent(aggregateId) => Future.successful(Right(Seq(NoOpEvent(aggregateId, newSequenceNumber))))
        case _: DoNothing                 => Future.successful(Right(Seq.empty))
        case fail: FailCommandProcessing =>
          Future.failed(fail.withError)
        case CreateExceptionThrowingEvent(aggregateId, throwable) =>
          Future.successful(Right(Seq(ExceptionThrowingEvent(aggregateId, newSequenceNumber, throwable))))
        case CreateUnserializableEvent(aggregateId, throwable) =>
          Future.successful(Right(Seq(UnserializableEvent(aggregateId, newSequenceNumber, throwable))))
        case _ =>
          throw new RuntimeException("Received unexpected message in command handler! This should not happen and indicates a bad test")
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
    override def readState(bytes: Array[Byte]): Option[State] = Json.parse(bytes).asOpt[State]
    override def writeState(agg: State): SerializedAggregate = SerializedAggregate(Json.toJson(agg).toString().getBytes(), Map.empty)
  }

  val eventWriter: SurgeEventWriteFormatting[BaseTestEvent] = (evt: BaseTestEvent) => {
    val key = s"${evt.aggregateId}:${evt.sequenceNumber}"
    val body = Json.toJson(evt).toString().getBytes()
    SerializedMessage(key, body, Map.empty)
  }

  val businessLogic: SurgeCommandModel[State, BaseTestCommand, Nothing, BaseTestEvent] =
    command.SurgeCommandModel(
      aggregateName = "CountAggregate",
      kafka = kafkaConfig,
      model = BusinessLogic,
      aggregateReadFormatting = aggregateFormatting,
      aggregateWriteFormatting = aggregateFormatting,
      eventWriteFormatting = eventWriter,
      aggregateValidator = { (_, _, _) => true },
      metrics = Metrics.globalMetricRegistry,
      tracer = NoopTracerFactory.create())
}

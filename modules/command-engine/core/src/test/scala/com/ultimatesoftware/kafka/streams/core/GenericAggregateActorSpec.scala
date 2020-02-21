// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import akka.Done
import akka.actor.{ ActorRef, ActorSystem, Props, ReceiveTimeout }
import akka.pattern._
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import com.ultimatesoftware.akka.cluster.Passivate
import com.ultimatesoftware.kafka.streams.core.GenericAggregateActor.Stop
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, KafkaStreamsKeyValueStore }
import com.ultimatesoftware.scala.core.monitoring.metrics.NoOpMetricsProvider
import com.ultimatesoftware.scala.oss.domain.AggregateSegment
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.{ BeforeAndAfterAll, Matchers, PartialFunctionValues, WordSpecLike }
import org.scalatestplus.mockito.MockitoSugar
import play.api.libs.json.{ JsValue, Json }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class GenericAggregateActorSpec extends TestKit(ActorSystem("GenericAggregateActorSpec")) with WordSpecLike with Matchers
  with BeforeAndAfterAll with MockitoSugar with TestBoundedContext with PartialFunctionValues {

  private implicit val timeout: Timeout = Timeout(10.seconds)
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private def testActor(aggregateId: UUID = UUID.randomUUID(), producerActor: KafkaProducerActor[UUID, State, BaseTestEvent, TimestampMeta],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue]): ActorRef = {

    val props = testActorProps(aggregateId, producerActor, aggregateKafkaStreamsImpl)
    system.actorOf(props)
  }

  private def testActorProps(aggregateId: UUID = UUID.randomUUID(), producerActor: KafkaProducerActor[UUID, State, BaseTestEvent, TimestampMeta],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue]): Props = {
    val metrics = GenericAggregateActor.createMetrics(NoOpMetricsProvider, "testAggregate")

    GenericAggregateActor.props(aggregateId, kafkaStreamsLogic, producerActor, metrics, aggregateKafkaStreamsImpl)
  }

  private def envelope(cmd: BaseTestCommand): GenericAggregateActor.CommandEnvelope[UUID, BaseTestCommand, TimestampMeta] = {
    GenericAggregateActor.CommandEnvelope(cmd.aggregateId, TimestampMeta(Instant.now.truncatedTo(ChronoUnit.SECONDS)), cmd)
  }

  private def mockAggregateKeyValueStore(contents: Map[String, Array[Byte]]): KafkaStreamsKeyValueStore[String, Array[Byte]] = {
    val mockStore = mock[KafkaStreamsKeyValueStore[String, Array[Byte]]]
    when(mockStore.get(anyString)).thenAnswer((invocation: InvocationOnMock) ⇒ {
      val id = invocation.getArgument[String](0)
      Future.successful(contents.get(id))
    })

    when(mockStore.range(anyString, anyString)).thenAnswer((invocation: InvocationOnMock) ⇒ {
      val from = invocation.getArgument[String](0)
      val to = invocation.getArgument[String](1)

      val results = contents.filterKeys { key ⇒
        from <= key && key <= to
      }

      Future.successful(results.toList)
    })

    mockStore
  }

  private def mockKafkaStreams(mockStateStore: KafkaStreamsKeyValueStore[String, Array[Byte]]): AggregateStateStoreKafkaStreams[JsValue] = {
    val mockStreams = mock[AggregateStateStoreKafkaStreams[JsValue]]
    when(mockStreams.aggregateQueryableStateStore).thenReturn(mockStateStore)
    when(mockStreams.substatesForAggregate(anyString)(any[ExecutionContext])).thenCallRealMethod

    mockStreams
  }
  private def mockKafkaStreams(stateStoreContents: Map[String, Array[Byte]]): AggregateStateStoreKafkaStreams[JsValue] = {
    val mockStateStore = mockAggregateKeyValueStore(stateStoreContents)
    mockKafkaStreams(mockStateStore)
  }

  private def defaultMockProducer: KafkaProducerActor[UUID, State, BaseTestEvent, TimestampMeta] = {
    val mockProducer = mock[KafkaProducerActor[UUID, State, BaseTestEvent, TimestampMeta]]
    when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
    when(mockProducer.publish(any[UUID], any[Seq[(String, AggregateSegment[UUID, State])]], any[Seq[(String, TimestampMeta, BaseTestEvent)]]))
      .thenReturn(Future.successful(Done))

    mockProducer
  }

  case class Publish(aggregateId: UUID)
  private def probeBackedMockProducer(probe: TestProbe): KafkaProducerActor[UUID, State, BaseTestEvent, TimestampMeta] = {
    val mockProducer = mock[KafkaProducerActor[UUID, State, BaseTestEvent, TimestampMeta]]
    when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
    when(mockProducer.publish(any[UUID], any[Seq[(String, AggregateSegment[UUID, State])]], any[Seq[(String, TimestampMeta, BaseTestEvent)]]))
      .thenAnswer((invocation: InvocationOnMock) ⇒ {
        val aggregateId = invocation.getArgument[UUID](0)
        (probe.ref ? Publish(aggregateId)).map(_ ⇒ Done)(ExecutionContext.global)
      })

    mockProducer
  }

  private def processIncrementCommand(actor: ActorRef, state: State, mockProducer: KafkaProducerActor[UUID, State, BaseTestEvent, TimestampMeta]): Unit = {
    val probe = TestProbe()

    val incrementCmd = Increment(state.aggregateId)
    val testEnvelope = envelope(incrementCmd)

    val expectedEvent = CountIncremented(state.aggregateId, 1, state.version + 1, testEnvelope.meta.timestamp)
    probe.send(actor, testEnvelope)

    val expectedState = BusinessLogic.handleEvent(Some(state), expectedEvent, testEnvelope.meta)

    probe.expectMsg(GenericAggregateActor.CommandSuccess(expectedState))

    val expectedStateKeyValues = expectedState.toSeq.flatMap { state ⇒
      val states = kafkaStreamsLogic.aggregateComposer.decompose(state.aggregateId, state).toSeq
      states.map(s ⇒ BusinessLogic.stateKeyExtractor(s.value) -> s)
    }

    val expectedKey = expectedEvent.aggregateId.toString + ":" + expectedEvent.sequenceNumber
    val expectedEventKeyVal = (expectedKey, testEnvelope.meta, expectedEvent)
    verify(mockProducer).publish(state.aggregateId, expectedStateKeyValues, Seq(expectedEventKeyVal))
  }

  object TestContext {
    def setupDefault: TestContext = {
      setupDefault()
    }

    def setupDefault(
      testAggregateId: UUID = UUID.randomUUID(),
      mockProducer: KafkaProducerActor[UUID, State, BaseTestEvent, TimestampMeta] = defaultMockProducer): TestContext = {
      val probe = TestProbe()

      val baseState = State(testAggregateId, 3, 3, Instant.now)
      val mockStreams = mockKafkaStreams(Map(testAggregateId.toString -> Json.toJson(baseState).toString().getBytes()))
      val actor = probe.childActorOf(testActorProps(testAggregateId, mockProducer, mockStreams))

      TestContext(probe, baseState, mockProducer, actor)
    }
  }
  case class TestContext(probe: TestProbe, baseState: State, mockProducer: KafkaProducerActor[UUID, State, BaseTestEvent, TimestampMeta], actor: ActorRef) {
    val testAggregateId: UUID = baseState.aggregateId
  }

  "GenericAggregateActor" should {
    "Properly initialize from Kafka streams" in {
      val testContext = TestContext.setupDefault
      import testContext._

      probe.send(actor, GenericAggregateActor.GetState(testAggregateId))
      probe.expectMsg(Some(baseState))

      processIncrementCommand(actor, baseState, mockProducer)
    }

    "Be able to initialize from multiple substates" in {

    }

    "Retry initialization if not up to date" in {
      val probe = TestProbe()

      val testAggregateId = UUID.randomUUID()
      val baseState = State(testAggregateId, 3, 3, Instant.now)

      val mockProducer = mock[KafkaProducerActor[UUID, State, BaseTestEvent, TimestampMeta]]
      when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(false), Future.successful(true))
      when(mockProducer.publish(any[UUID], any[Seq[(String, AggregateSegment[UUID, State])]],
        any[Seq[(String, TimestampMeta, BaseTestEvent)]])).thenReturn(Future.successful(Done))

      val mockStreams = mockKafkaStreams(Map(testAggregateId.toString -> Json.toJson(baseState).toString().getBytes()))

      val actor = testActor(testAggregateId, mockProducer, mockStreams)

      probe.send(actor, GenericAggregateActor.GetState(testAggregateId))
      probe.expectMsg(Some(baseState))
    }

    "Retry initialization on a failure to read from the KTable" in {
      val probe = TestProbe()

      val testAggregateId = UUID.randomUUID()
      val baseState = State(testAggregateId, 3, 3, Instant.now)

      val mockProducer = defaultMockProducer

      val mockStore = mock[KafkaStreamsKeyValueStore[String, Array[Byte]]]
      val mockStreams = mockKafkaStreams(mockStore)
      when(mockStore.get(testAggregateId.toString)).thenReturn(
        Future.failed[Option[Array[Byte]]](new RuntimeException("This is expected")),
        Future.successful(Some(Json.toJson(baseState).toString().getBytes())))

      val actor = testActor(testAggregateId, mockProducer, mockStreams)

      probe.send(actor, GenericAggregateActor.GetState(testAggregateId))
      probe.expectMsg(5.seconds, Some(baseState))
    }

    "Handle validation failures by returning a CommandFailure" in {
      val testContext = TestContext.setupDefault
      import testContext._

      val validationCmd = CauseInvalidValidation(testAggregateId)
      val testEnvelope = envelope(validationCmd)

      probe.send(actor, testEnvelope)
      probe.expectMsg(GenericAggregateActor.CommandFailure(validationCmd.validationErrors))
    }

    "Not update state if there are no events processed" in {
      val testContext = TestContext.setupDefault
      import testContext._

      val testEnvelope = envelope(DoNothing(testAggregateId))

      probe.send(actor, testEnvelope)
      probe.expectMsg(GenericAggregateActor.CommandSuccess(Some(baseState)))

      verify(mockProducer, never()).publish(
        any[UUID],
        any[Seq[(String, AggregateSegment[UUID, State])]],
        any[Seq[(String, TimestampMeta, BaseTestEvent)]])
    }

    "Handle exceptions from the domain by returning a CommandError" in {
      val testContext = TestContext.setupDefault
      import testContext._

      val testException = new RuntimeException("This is an expected exception")
      val validationCmd = FailCommandProcessing(testAggregateId, testException)
      val testEnvelope = envelope(validationCmd)

      probe.send(actor, testEnvelope)
      probe.expectMsg(GenericAggregateActor.CommandError(testException))
    }

    "Process commands one at a time" in {
      val producerProbe = TestProbe()
      val testContext = TestContext.setupDefault(mockProducer = probeBackedMockProducer(producerProbe))
      import testContext._

      val probe = TestProbe()

      val incrementCmd = Increment(baseState.aggregateId)
      val testEnvelope = envelope(incrementCmd)

      val expectedEvent1 = CountIncremented(baseState.aggregateId, 1, baseState.version + 1, testEnvelope.meta.timestamp)
      val expectedState1 = BusinessLogic.handleEvent(Some(baseState), expectedEvent1, testEnvelope.meta)

      val expectedEvent2 = CountIncremented(expectedState1.get.aggregateId, 1, expectedState1.get.version + 1, testEnvelope.meta.timestamp)
      val expectedState2 = BusinessLogic.handleEvent(expectedState1, expectedEvent2, testEnvelope.meta)

      probe.send(actor, testEnvelope)
      actor ! ReceiveTimeout // This should be ignored while the actor is processing a command
      probe.send(actor, testEnvelope)

      producerProbe.expectMsg(Publish(testAggregateId))
      producerProbe.reply(Done)
      probe.expectMsg(GenericAggregateActor.CommandSuccess(expectedState1))

      producerProbe.expectMsg(Publish(testAggregateId))
      producerProbe.reply(Done)
      probe.expectMsg(GenericAggregateActor.CommandSuccess(expectedState2))
    }

    "Crash the actor to force reinitialization if publishing events hits an error" in {
      val crashingMockProducer = mock[KafkaProducerActor[UUID, State, BaseTestEvent, TimestampMeta]]
      when(crashingMockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
      when(crashingMockProducer.publish(any[UUID], any[Seq[(String, AggregateSegment[UUID, State])]], any[Seq[(String, TimestampMeta, BaseTestEvent)]]))
        .thenReturn(Future.failed(new RuntimeException("This is expected")))

      val testContext = TestContext.setupDefault(mockProducer = crashingMockProducer)
      import testContext._

      probe.watch(actor)

      val incrementCmd = Increment(baseState.aggregateId)
      val testEnvelope = envelope(incrementCmd)
      probe.send(actor, testEnvelope)
      probe.expectTerminated(actor)
    }

    "Be able to correctly extract the correct aggregate ID from messages" in {
      val command1 = GenericAggregateActor.CommandEnvelope(aggregateId = "foobarbaz", meta = TimestampMeta(Instant.now),
        command = "unused")
      val command2 = GenericAggregateActor.CommandEnvelope(
        aggregateId = UUID.randomUUID(),
        meta = TimestampMeta(Instant.now), command = "unused")

      val getState1 = GenericAggregateActor.GetState(aggregateId = "foobarbaz")
      val getState2 = GenericAggregateActor.GetState(aggregateId = UUID.randomUUID())

      GenericAggregateActor.RoutableMessage.extractEntityId[String](command1) shouldEqual command1.aggregateId
      GenericAggregateActor.RoutableMessage.extractEntityId[UUID](command2) shouldEqual command2.aggregateId

      GenericAggregateActor.RoutableMessage.extractEntityId[String](getState1) shouldEqual getState1.aggregateId
      GenericAggregateActor.RoutableMessage.extractEntityId[UUID](getState2) shouldEqual getState2.aggregateId
    }

    "Passivate after the actor idle timeout threshold is exceeded" in {
      val testContext = TestContext.setupDefault
      import testContext._

      probe.send(actor, ReceiveTimeout) // When uninitialized, the actor should ignore a ReceiveTimeout
      probe.expectNoMessage()

      processIncrementCommand(actor, baseState, mockProducer)

      probe.watch(actor)

      actor ! ReceiveTimeout
      probe.expectMsg(Passivate(Stop))
      probe.reply(Stop)

      probe.expectTerminated(actor)
    }
  }
}

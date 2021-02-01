// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, NoSerializationVerificationNeeded, Props, ReceiveTimeout }
import akka.pattern._
import akka.serialization.Serializers
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import org.apache.kafka.common.header.internals.RecordHeaders
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.{ ArgumentCaptor, ArgumentMatchers }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{ BeforeAndAfterAll, PartialFunctionValues }
import org.scalatestplus.mockito.MockitoSugar
import play.api.libs.json.{ JsValue, Json }
import surge.akka.cluster.Passivate
import surge.core.GenericAggregateActor.{ ApplyEventEnvelope, CommandError, Stop }
import surge.kafka.streams.{ AggregateStateStoreKafkaStreams, KafkaStreamsKeyValueStore }
import surge.metrics.NoOpMetricsProvider
import surge.scala.core.kafka.HeadersHelper

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import org.mockito.ArgumentMatcher
class IsAtLeastOneElementSeq extends ArgumentMatcher[Seq[KafkaProducerActor.MessageToPublish]] {
  def matches(seq: Seq[KafkaProducerActor.MessageToPublish]): Boolean = seq.nonEmpty
}

class GenericAggregateActorSpec extends TestKit(ActorSystem("GenericAggregateActorSpec")) with AnyWordSpecLike with Matchers
  with BeforeAndAfterAll with MockitoSugar with TestBoundedContext with PartialFunctionValues {
  import ArgumentMatchers.{ any, anyString, eq => argEquals }
  import TestBoundedContext._

  private implicit val timeout: Timeout = Timeout(10.seconds)
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def randomUUID: String = UUID.randomUUID().toString

  private def testActor(aggregateId: String = randomUUID, producerActor: KafkaProducerActor[State, BaseTestEvent],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue], publishStateOnly: Boolean = false): ActorRef = {

    val props = testActorProps(aggregateId, producerActor, aggregateKafkaStreamsImpl, publishStateOnly)
    system.actorOf(props)
  }

  private def testActorProps(aggregateId: String = randomUUID, producerActor: KafkaProducerActor[State, BaseTestEvent],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    publishStateOnly: Boolean = false): Props = {
    val metrics = GenericAggregateActor.createMetrics(NoOpMetricsProvider, "testAggregate")

    GenericAggregateActor.props(aggregateId, businessLogic.copy(kafka = businessLogic.kafka.copy(publishStateOnly = publishStateOnly)), producerActor, metrics, aggregateKafkaStreamsImpl)
  }

  private def envelope(cmd: BaseTestCommand): GenericAggregateActor.CommandEnvelope[BaseTestCommand] = {
    GenericAggregateActor.CommandEnvelope(cmd.aggregateId, cmd)
  }

  private def eventEnvelope(event: BaseTestEvent): GenericAggregateActor.ApplyEventEnvelope[BaseTestEvent] = {
    GenericAggregateActor.ApplyEventEnvelope[BaseTestEvent](event.aggregateId, event)
  }

  private def mockKafkaStreams(state: State): AggregateStateStoreKafkaStreams[JsValue] = {
    val mockStreams = mock[AggregateStateStoreKafkaStreams[JsValue]]
    when(mockStreams.getAggregateBytes(anyString)).thenReturn(Future.successful(Some(Json.toJson(state).toString().getBytes())))
    when(mockStreams.substatesForAggregate(anyString)(any[ExecutionContext])).thenReturn(Future.successful(List()))

    mockStreams
  }

  private def defaultMockProducer: KafkaProducerActor[State, BaseTestEvent] = {
    val mockProducer = mock[KafkaProducerActor[State, BaseTestEvent]]
    when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
    when(mockProducer.publish(anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
      .thenReturn(Future.successful(KafkaProducerActor.PublishSuccess))

    mockProducer
  }

  case class Publish(aggregateId: String) extends NoSerializationVerificationNeeded
  private def probeBackedMockProducer(probe: TestProbe): KafkaProducerActor[State, BaseTestEvent] = {
    val mockProducer = mock[KafkaProducerActor[State, BaseTestEvent]]
    when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
    when(mockProducer.publish(anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
      .thenAnswer((invocation: InvocationOnMock) => {
        val aggregateId = invocation.getArgument[String](0)
        (probe.ref ? Publish(aggregateId)).map(_ => KafkaProducerActor.PublishSuccess)(ExecutionContext.global)
      })

    mockProducer
  }

  private def processIncrementCommand(actor: ActorRef, state: State, mockProducer: KafkaProducerActor[State, BaseTestEvent]): Unit = {
    val probe = TestProbe()

    val incrementCmd = Increment(state.aggregateId)
    val testEnvelope = envelope(incrementCmd)

    val expectedEvent = CountIncremented(state.aggregateId, 1, state.version + 1)
    probe.send(actor, testEnvelope)

    val expectedState = BusinessLogic.handleEvent(Some(state), expectedEvent)

    probe.expectMsg(GenericAggregateActor.CommandSuccess(expectedState))

    val serializedEvent = businessLogic.writeFormatting.writeEvent(expectedEvent)
    val serializedAgg = expectedState.map(businessLogic.writeFormatting.writeState)
    val expectedStateSerialized = KafkaProducerActor.MessageToPublish(state.aggregateId.toString, serializedAgg.map(_.value).orNull, new RecordHeaders())
    val headers = HeadersHelper.createHeaders(serializedEvent.headers)
    val expectedEventSerialized = KafkaProducerActor.MessageToPublish(serializedEvent.key, serializedEvent.value, headers)

    val stateValueCaptor: ArgumentCaptor[KafkaProducerActor.MessageToPublish] = ArgumentCaptor.forClass(classOf[KafkaProducerActor.MessageToPublish])
    val eventsCaptor: ArgumentCaptor[Seq[KafkaProducerActor.MessageToPublish]] = ArgumentCaptor.forClass(classOf[Seq[KafkaProducerActor.MessageToPublish]])
    verify(mockProducer).publish(argEquals(state.aggregateId), stateValueCaptor.capture(), eventsCaptor.capture())

    // Need to compare the individual values here since the byte array comparison looks at object references rather than actual bytes
    stateValueCaptor.getValue.key shouldEqual expectedStateSerialized.key
    stateValueCaptor.getValue.value shouldEqual expectedStateSerialized.value

    eventsCaptor.getValue.head.key shouldEqual expectedEventSerialized.key
    eventsCaptor.getValue.head.value shouldEqual expectedEventSerialized.value
    eventsCaptor.getValue.head.headers shouldEqual expectedEventSerialized.headers
  }

  object TestContext {
    def setupDefault: TestContext = {
      setupDefault()
    }

    def setupDefault(
      testAggregateId: String = randomUUID,
      mockProducer: KafkaProducerActor[State, BaseTestEvent] = defaultMockProducer,
      publishStateOnly: Boolean = false): TestContext = {
      val probe = TestProbe()

      val baseState = State(testAggregateId, 3, 3)
      val mockStreams = mockKafkaStreams(baseState)
      val actor = probe.childActorOf(testActorProps(testAggregateId, mockProducer, mockStreams, publishStateOnly = publishStateOnly))

      TestContext(probe, baseState, mockProducer, actor, publishStateOnly)
    }
  }
  case class TestContext(probe: TestProbe, baseState: State, mockProducer: KafkaProducerActor[State, BaseTestEvent], actor: ActorRef,
      publishStateOnly: Boolean) {
    val testAggregateId: String = baseState.aggregateId
  }

  "GenericAggregateActor" should {
    "Properly initialize from Kafka streams" in {
      val testContext = TestContext.setupDefault
      import testContext._

      probe.send(actor, GenericAggregateActor.GetState(testAggregateId))
      probe.expectMsg(GenericAggregateActor.StateResponse(Some(baseState)))

      processIncrementCommand(actor, baseState, mockProducer)
    }

    "Not Publish" should {
      "Anything when state did not change and publishStateOnly is false" in {
        val testContext = TestContext.setupDefault
        import testContext._

        val probe = TestProbe()

        val doNothingCmd = DoNothing(baseState.aggregateId)
        val testEnvelope = envelope(doNothingCmd)
        probe.send(actor, testEnvelope)

        probe.expectMsg(GenericAggregateActor.CommandSuccess(Some(baseState)))

        verify(testContext.mockProducer, never()).publish(
          ArgumentMatchers.eq(testAggregateId),
          ArgumentMatchers.any[KafkaProducerActor.MessageToPublish](),
          ArgumentMatchers.eq(Seq[KafkaProducerActor.MessageToPublish]()))

      }

      "Anything when state did not change and publishStateOnly is true" in {
        val testContext = TestContext.setupDefault(publishStateOnly = true)
        import testContext._

        val probe = TestProbe()

        val doNothingCmd = DoNothing(baseState.aggregateId)
        val testEnvelope = envelope(doNothingCmd)
        probe.send(actor, testEnvelope)

        probe.expectMsg(GenericAggregateActor.CommandSuccess(Some(baseState)))

        verify(testContext.mockProducer, never()).publish(
          ArgumentMatchers.eq(testAggregateId),
          ArgumentMatchers.any[KafkaProducerActor.MessageToPublish](),
          ArgumentMatchers.eq(Seq[KafkaProducerActor.MessageToPublish]()))

      }

      "Anything when there are no events processed and publishStateOnly is false" in {
        val testContext = TestContext.setupDefault
        import testContext._

        val testEnvelope = envelope(DoNothing(testAggregateId))

        probe.send(actor, testEnvelope)
        probe.expectMsg(GenericAggregateActor.CommandSuccess(Some(baseState)))

        verify(mockProducer, never()).publish(
          any[String],
          any[KafkaProducerActor.MessageToPublish],
          any[Seq[KafkaProducerActor.MessageToPublish]])
      }

      "Anything when state has not changed and publishStateOnly is false" in {

        val producerProbe = TestProbe()
        val testContext = TestContext.setupDefault(mockProducer = probeBackedMockProducer(producerProbe))
        import testContext._

        val testEnvelope: ApplyEventEnvelope[CountIncremented] =
          ApplyEventEnvelope[CountIncremented](testAggregateId, CountIncremented(testAggregateId, 0, 3))

        probe.send(actor, testEnvelope)
        probe.expectMsg(GenericAggregateActor.CommandSuccess(Some(baseState)))

        verify(mockProducer, never()).publish(
          ArgumentMatchers.eq(testAggregateId),
          ArgumentMatchers.any(classOf[KafkaProducerActor.MessageToPublish]),
          ArgumentMatchers.argThat(new IsAtLeastOneElementSeq()))
      }
    }

    "Publish" should {
      "Everything when publishStateOnly is false" in {
        val testContext = TestContext.setupDefault
        import testContext._

        val probe = TestProbe()

        val incrementCmd = Increment(baseState.aggregateId)
        val testEnvelope = envelope(incrementCmd)
        probe.send(actor, testEnvelope)

        val publishedState = baseState.copy(
          count = baseState.count + 1,
          version = baseState.version + 1)

        probe.expectMsg(GenericAggregateActor.CommandSuccess(Some(publishedState)))

        verify(testContext.mockProducer, times(1)).publish(
          ArgumentMatchers.eq(testAggregateId),
          ArgumentMatchers.any[KafkaProducerActor.MessageToPublish](),
          ArgumentMatchers.argThat(new IsAtLeastOneElementSeq()))
      }

      "Only stateChange when publishStateOnly is true" in {
        val testContext = TestContext.setupDefault(publishStateOnly = true)
        import testContext._

        val probe = TestProbe()

        val incrementCmd = Increment(baseState.aggregateId)
        val testEnvelope = envelope(incrementCmd)
        probe.send(actor, testEnvelope)

        val publishedState = baseState.copy(
          count = baseState.count + 1,
          version = baseState.version + 1)

        probe.expectMsg(GenericAggregateActor.CommandSuccess(Some(publishedState)))

        verify(testContext.mockProducer, times(1)).publish(
          ArgumentMatchers.eq(testAggregateId),
          ArgumentMatchers.any[KafkaProducerActor.MessageToPublish](),
          ArgumentMatchers.eq(Seq[KafkaProducerActor.MessageToPublish]()))
      }
    }

    "Retry initialization" should {
      "if not up to date" in {
        val probe = TestProbe()

        val testAggregateId = UUID.randomUUID().toString
        val baseState = State(testAggregateId, 3, 3)

        val mockProducer = mock[KafkaProducerActor[State, BaseTestEvent]]
        when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(false), Future.successful(true))
        when(mockProducer.publish(anyString, any[KafkaProducerActor.MessageToPublish],
          any[Seq[KafkaProducerActor.MessageToPublish]])).thenReturn(Future.successful(KafkaProducerActor.PublishSuccess))

        val mockStreams = mockKafkaStreams(baseState)

        val actor = testActor(testAggregateId, mockProducer, mockStreams)

        probe.send(actor, GenericAggregateActor.GetState(testAggregateId))
        probe.expectMsg(GenericAggregateActor.StateResponse(Some(baseState)))
      }

      "if up to date check fails" in {
        val probe = TestProbe()

        val testAggregateId = UUID.randomUUID().toString
        val baseState = State(testAggregateId, 3, 3)

        val mockProducer = mock[KafkaProducerActor[State, BaseTestEvent]]
        when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.failed(new RuntimeException("This is expected")), Future.successful(true))
        when(mockProducer.publish(anyString, any[KafkaProducerActor.MessageToPublish],
          any[Seq[KafkaProducerActor.MessageToPublish]])).thenReturn(Future.successful(KafkaProducerActor.PublishSuccess))

        val mockStreams = mockKafkaStreams(baseState)

        val actor = testActor(testAggregateId, mockProducer, mockStreams)

        probe.send(actor, GenericAggregateActor.GetState(testAggregateId))
        probe.expectMsg(6.seconds, GenericAggregateActor.StateResponse(Some(baseState)))
      }

      "on a failure to read from the KTable" in {
        val probe = TestProbe()

        val testAggregateId = UUID.randomUUID().toString
        val baseState = State(testAggregateId, 3, 3)

        val mockProducer = defaultMockProducer

        val mockStore = mock[KafkaStreamsKeyValueStore[String, Array[Byte]]]
        val mockStreams = mockKafkaStreams(baseState)
        when(mockStore.get(testAggregateId.toString)).thenReturn(
          Future.failed[Option[Array[Byte]]](new RuntimeException("This is expected")),
          Future.successful(Some(Json.toJson(baseState).toString().getBytes())))

        val actor = testActor(testAggregateId, mockProducer, mockStreams)

        probe.send(actor, GenericAggregateActor.GetState(testAggregateId))
        probe.expectMsg(5.seconds, GenericAggregateActor.StateResponse(Some(baseState)))
      }
    }

    "Not update state if there are no events processed" in {
      val testContext = TestContext.setupDefault
      import testContext._

      val testEnvelope = envelope(DoNothing(testAggregateId))

      probe.send(actor, testEnvelope)
      probe.expectMsg(GenericAggregateActor.CommandSuccess(Some(baseState)))

      verify(mockProducer, never()).publish(
        anyString,
        any[KafkaProducerActor.MessageToPublish],
        any[Seq[KafkaProducerActor.MessageToPublish]])
    }

    "Handle exceptions" should {
      "from the domain by returning a CommandError" in {
        val testContext = TestContext.setupDefault
        import testContext._

        val testException = new RuntimeException("This is an expected exception")
        val validationCmd = FailCommandProcessing(testAggregateId, testException)
        val testEnvelope = envelope(validationCmd)

        probe.send(actor, testEnvelope)
        val commandError = probe.expectMsgClass(classOf[GenericAggregateActor.CommandError])
        // Fuzzy matching because serializing and deserializing gets a different object and messes up .equals even though the two are identical
        commandError.exception shouldBe a[RuntimeException]
        commandError.exception.getMessage shouldEqual testException.getMessage
      }
    }

    "Process commands one at a time" in {
      val producerProbe = TestProbe()
      val testContext = TestContext.setupDefault(mockProducer = probeBackedMockProducer(producerProbe))
      import testContext._

      val probe = TestProbe()

      val incrementCmd = Increment(baseState.aggregateId)
      val testEnvelope = envelope(incrementCmd)

      val expectedEvent1 = CountIncremented(baseState.aggregateId, 1, baseState.version + 1)
      val expectedState1 = BusinessLogic.handleEvent(Some(baseState), expectedEvent1)

      val expectedEvent2 = CountIncremented(expectedState1.get.aggregateId, 1, expectedState1.get.version + 1)
      val expectedState2 = BusinessLogic.handleEvent(expectedState1, expectedEvent2)

      probe.send(actor, testEnvelope)
      actor ! ReceiveTimeout // This should be ignored while the actor is processing a command
      probe.send(actor, testEnvelope)

      producerProbe.expectMsg(Publish(testAggregateId))
      producerProbe.reply(KafkaProducerActor.PublishSuccess)
      probe.expectMsg(GenericAggregateActor.CommandSuccess(expectedState1))

      producerProbe.expectMsg(Publish(testAggregateId))
      producerProbe.reply(KafkaProducerActor.PublishSuccess)
      probe.expectMsg(GenericAggregateActor.CommandSuccess(expectedState2))
    }

    "Crash the actor to force reinitialization if publishing events times out" in {
      val crashingMockProducer = mock[KafkaProducerActor[State, BaseTestEvent]]
      val expectedException = new RuntimeException("This is expected")

      when(crashingMockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
      when(crashingMockProducer.publish(anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
        .thenReturn(Future.failed(expectedException))

      val testContext = TestContext.setupDefault(mockProducer = crashingMockProducer)
      import testContext._

      probe.watch(actor)

      val incrementCmd = Increment(baseState.aggregateId)
      val testEnvelope = envelope(incrementCmd)
      probe.send(actor, testEnvelope)
      probe.expectMsg(CommandError(expectedException))
      probe.expectTerminated(actor)
    }

    "Wrap and return the error from publishing to Kafka if publishing explicitly fails consistently" in {
      val failingMockProducer = mock[KafkaProducerActor[State, BaseTestEvent]]
      val expectedException = new RuntimeException("This is expected")
      when(failingMockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
      when(failingMockProducer.publish(anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
        .thenReturn(Future.successful(KafkaProducerActor.PublishFailure(expectedException)))

      val testContext = TestContext.setupDefault(mockProducer = failingMockProducer)
      import testContext._

      probe.watch(actor)

      val incrementCmd = Increment(baseState.aggregateId)
      val testEnvelope = envelope(incrementCmd)
      probe.send(actor, testEnvelope)
      probe.expectMsg(CommandError(expectedException))
      probe.expectTerminated(actor)
    }

    "Retry publishing to Kafka if publishing explicitly fails" in {
      val failingMockProducer = mock[KafkaProducerActor[State, BaseTestEvent]]
      val expectedException = new RuntimeException("This is expected")
      when(failingMockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
      when(failingMockProducer.publish(anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
        .thenReturn(Future.successful(KafkaProducerActor.PublishFailure(expectedException)))
        .thenReturn(Future.successful(KafkaProducerActor.PublishSuccess))

      val testContext = TestContext.setupDefault(mockProducer = failingMockProducer)
      import testContext._

      val incrementCmd = Increment(baseState.aggregateId)
      val testEnvelope = envelope(incrementCmd)
      val expectedEvent = CountIncremented(baseState.aggregateId, 1, baseState.version + 1)
      val expectedState = BusinessLogic.handleEvent(Some(baseState), expectedEvent)

      probe.send(actor, testEnvelope)
      probe.expectMsg(GenericAggregateActor.CommandSuccess(expectedState))
    }

    "Be able to correctly extract the correct aggregate ID from messages" in {
      val command1 = GenericAggregateActor.CommandEnvelope(
        aggregateId = "foobarbaz",
        command = "unused")
      val command2 = GenericAggregateActor.CommandEnvelope(
        aggregateId = randomUUID, command = "unused")

      val getState1 = GenericAggregateActor.GetState(aggregateId = "foobarbaz")
      val getState2 = GenericAggregateActor.GetState(aggregateId = randomUUID)

      GenericAggregateActor.RoutableMessage.extractEntityId(command1) shouldEqual command1.aggregateId
      GenericAggregateActor.RoutableMessage.extractEntityId(command2) shouldEqual command2.aggregateId

      GenericAggregateActor.RoutableMessage.extractEntityId(getState1) shouldEqual getState1.aggregateId
      GenericAggregateActor.RoutableMessage.extractEntityId(getState2) shouldEqual getState2.aggregateId
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

    "Serialize/Deserialize a CommandEnvelope from Akka" in {
      import akka.serialization.SerializationExtension
      def doSerde[A](envelope: GenericAggregateActor.CommandEnvelope[A]): Unit = {
        val serialization = SerializationExtension.get(system)
        val serializer = serialization.findSerializerFor(envelope)
        val serialized = serialization.serialize(envelope).get
        val manifest = Serializers.manifestFor(serializer, envelope)
        val deserialized = serialization.deserialize(serialized, serializer.identifier, manifest).get
        deserialized shouldEqual envelope
      }
      doSerde(GenericAggregateActor.CommandEnvelope[String]("hello", "test2"))
      doSerde(envelope(Increment(UUID.randomUUID().toString)))
    }

    "Serialize/Deserialize an ApplyEventEnvelope from Akka" in {
      import akka.serialization.SerializationExtension

      def doSerde[A](envelope: ApplyEventEnvelope[A]): Unit = {
        val serialization = SerializationExtension.get(system)
        val serializer = serialization.findSerializerFor(envelope)
        val serialized = serialization.serialize(envelope).get
        val manifest = Serializers.manifestFor(serializer, envelope)
        val deserialized = serialization.deserialize(serialized, serializer.identifier, manifest).get
        deserialized shouldEqual envelope
      }

      doSerde(GenericAggregateActor.ApplyEventEnvelope[String](UUID.randomUUID().toString, "test2"))
      doSerde(eventEnvelope(CountIncremented(UUID.randomUUID().toString, 1, 1)))
    }
  }
}

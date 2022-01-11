// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import akka.actor.{ ActorRef, ActorSystem, NoSerializationVerificationNeeded, Props, ReceiveTimeout }
import akka.pattern._
import akka.serialization.{ SerializationExtension, Serializers }
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.{ ArgumentCaptor, ArgumentMatcher, ArgumentMatchers }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{ BeforeAndAfterAll, PartialFunctionValues }
import org.scalatestplus.mockito.MockitoSugar
import play.api.libs.json.Json
import surge.akka.cluster.Passivate
import surge.core.KafkaProducerActor.{ MessageTracker, MessageTrackerWithExpiry, PublishSuccess }
import surge.core.{ KafkaProducerActor, TestBoundedContext }
import surge.exceptions.{ AggregateInitializationException, KafkaPublishTimeoutException }
import surge.internal.kafka.HeadersHelper
import surge.internal.persistence.PersistentActor.{ ACKError, ApplyEvents, Stop }
import surge.internal.tracing.RoutableMessage
import surge.kafka.streams.{ AggregateStateStoreKafkaStreams, ExpectedTestException }
import surge.metrics.Metrics

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class IsAtLeastOneElementSeq extends ArgumentMatcher[Seq[KafkaProducerActor.MessageToPublish]] {
  def matches(seq: Seq[KafkaProducerActor.MessageToPublish]): Boolean = seq.nonEmpty
}

class PersistentActorSpec
    extends TestKit(ActorSystem("PersistentActorSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar
    with TestBoundedContext
    with PartialFunctionValues {
  import ArgumentMatchers.{ any, anyString, eq => argEquals }
  import TestBoundedContext._

  private implicit val timeout: Timeout = Timeout(10.seconds)
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  def randomUUID: String = UUID.randomUUID().toString

  private def testActor(
      aggregateId: String,
      producerActor: KafkaProducerActor,
      aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams,
      publishStateOnly: Boolean = false): ActorRef = {

    val props = testActorProps(aggregateId, producerActor, aggregateKafkaStreamsImpl, publishStateOnly)
    system.actorOf(props)
  }

  private def testActorProps(
      aggregateId: String,
      producerActor: KafkaProducerActor,
      aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams,
      publishStateOnly: Boolean = false): Props = {
    val metrics = PersistentActor.createMetrics(Metrics.globalMetricRegistry, "testAggregate")
    val aggregateIdToKafkaProducer = (_: String) => producerActor
    val sharedResources = PersistentEntitySharedResources(aggregateIdToKafkaProducer, metrics, aggregateKafkaStreamsImpl)
    PersistentActor.props(
      businessLogic.copy(kafka = businessLogic.kafka.copy(publishStateOnly = publishStateOnly)),
      sharedResources,
      ConfigFactory.load(),
      Some(aggregateId))
  }

  private def envelope(cmd: BaseTestCommand): PersistentActor.ProcessMessage[BaseTestCommand] = {
    PersistentActor.ProcessMessage(cmd.aggregateId, cmd)
  }

  private def eventEnvelope(event: BaseTestEvent): PersistentActor.ApplyEvents[BaseTestEvent] = {
    PersistentActor.ApplyEvents[BaseTestEvent](event.aggregateId, List(event))
  }

  private def mockKafkaStreams(state: State): AggregateStateStoreKafkaStreams = {
    val mockStreams = mock[AggregateStateStoreKafkaStreams]
    when(mockStreams.getAggregateBytes(anyString)).thenReturn(Future.successful(Some(Json.toJson(state).toString().getBytes())))
    when(mockStreams.substatesForAggregate(anyString)(any[ExecutionContext])).thenReturn(Future.successful(List()))

    mockStreams
  }

  private def defaultMockProducer: KafkaProducerActor = {
    val mockProducer = mock[KafkaProducerActor]
    when(mockProducer.assignedPartition).thenReturn(new TopicPartition("TestTopic", 1))
    when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
    when(mockProducer.publish(any[UUID], anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
      .thenReturn(Future.successful(KafkaProducerActor.PublishSuccess))

    mockProducer
  }

  case class Publish(aggregateId: String) extends NoSerializationVerificationNeeded
  private def probeBackedMockProducer(probe: TestProbe): KafkaProducerActor = {
    val mockProducer = mock[KafkaProducerActor]
    when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
    when(mockProducer.publish(any[UUID], anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]])).thenAnswer(
      (invocation: InvocationOnMock) => {
        val aggregateId = invocation.getArgument[String](1)
        (probe.ref ? Publish(aggregateId)).map(_ => KafkaProducerActor.PublishSuccess)(ExecutionContext.global)
      })

    mockProducer
  }

  private def processIncrementCommand(actor: ActorRef, state: State, mockProducer: KafkaProducerActor): Unit = {
    val probe = TestProbe()

    val incrementCmd = Increment(state.aggregateId)
    val testEnvelope = envelope(incrementCmd)

    val expectedEvent = CountIncremented(state.aggregateId, 1, state.version + 1)
    probe.send(actor, testEnvelope)

    val expectedState = BusinessLogic.handleEvent(Some(state), expectedEvent)

    probe.expectMsg(PersistentActor.ACKSuccess(expectedState))
    val serializedEvent = businessLogic.eventWriteFormatting.writeEvent(expectedEvent)
    val serializedAgg = expectedState.map(businessLogic.aggregateWriteFormatting.writeState)
    val expectedStateSerialized = KafkaProducerActor.MessageToPublish(state.aggregateId, serializedAgg.map(_.value).orNull, new RecordHeaders())
    val headers = HeadersHelper.createHeaders(serializedEvent.headers)
    val expectedEventSerialized = KafkaProducerActor.MessageToPublish(serializedEvent.key, serializedEvent.value, headers)

    val stateValueCaptor: ArgumentCaptor[KafkaProducerActor.MessageToPublish] = ArgumentCaptor.forClass(classOf[KafkaProducerActor.MessageToPublish])
    val eventsCaptor: ArgumentCaptor[Seq[KafkaProducerActor.MessageToPublish]] = ArgumentCaptor.forClass(classOf[Seq[KafkaProducerActor.MessageToPublish]])
    verify(mockProducer).publish(any[UUID], argEquals(state.aggregateId), stateValueCaptor.capture(), eventsCaptor.capture())

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
        mockProducer: KafkaProducerActor = defaultMockProducer,
        publishStateOnly: Boolean = false): TestContext = {
      val probe = TestProbe()

      val baseState = State(testAggregateId, 3, 3)
      val mockStreams = mockKafkaStreams(baseState)
      val actor = probe.childActorOf(testActorProps(testAggregateId, mockProducer, mockStreams, publishStateOnly = publishStateOnly))

      TestContext(probe, baseState, mockProducer, actor, publishStateOnly)
    }
  }
  case class TestContext(probe: TestProbe, baseState: State, mockProducer: KafkaProducerActor, actor: ActorRef, publishStateOnly: Boolean) {
    val testAggregateId: String = baseState.aggregateId
  }

  "PersistentActor" should {
    "Properly initialize from Kafka streams" in {
      val testContext = TestContext.setupDefault
      import testContext._

      probe.send(actor, PersistentActor.GetState(testAggregateId))
      probe.expectMsg(PersistentActor.StateResponse(Some(baseState)))

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

        probe.expectMsg(PersistentActor.ACKSuccess(Some(baseState)))

        verify(testContext.mockProducer, never()).publish(
          any[UUID],
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

        probe.expectMsg(PersistentActor.ACKSuccess(Some(baseState)))

        verify(testContext.mockProducer, never()).publish(
          any[UUID],
          ArgumentMatchers.eq(testAggregateId),
          ArgumentMatchers.any[KafkaProducerActor.MessageToPublish](),
          ArgumentMatchers.eq(Seq[KafkaProducerActor.MessageToPublish]()))

      }

      "Anything when there are no events processed and publishStateOnly is false" in {
        val testContext = TestContext.setupDefault
        import testContext._

        val testEnvelope = envelope(DoNothing(testAggregateId))

        probe.send(actor, testEnvelope)
        probe.expectMsg(PersistentActor.ACKSuccess(Some(baseState)))

        verify(mockProducer, never()).publish(any[UUID], any[String], any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]])
      }

      "Anything when state has not changed and publishStateOnly is false" in {

        val producerProbe = TestProbe()
        val testContext = TestContext.setupDefault(mockProducer = probeBackedMockProducer(producerProbe))
        import testContext._

        val testEnvelope: ApplyEvents[CountIncremented] =
          ApplyEvents[CountIncremented](testAggregateId, List(CountIncremented(testAggregateId, 0, 3)))

        probe.send(actor, testEnvelope)
        probe.expectMsg(PersistentActor.ACKSuccess(Some(baseState)))

        verify(mockProducer, never()).publish(
          any[UUID],
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

        val publishedState = baseState.copy(count = baseState.count + 1, version = baseState.version + 1)

        probe.expectMsg(PersistentActor.ACKSuccess(Some(publishedState)))

        verify(testContext.mockProducer, times(1)).publish(
          any[UUID],
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

        val publishedState = baseState.copy(count = baseState.count + 1, version = baseState.version + 1)

        probe.expectMsg(PersistentActor.ACKSuccess(Some(publishedState)))

        verify(testContext.mockProducer, times(1)).publish(
          any[UUID],
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

        val mockProducer = mock[KafkaProducerActor]
        when(mockProducer.assignedPartition).thenReturn(new TopicPartition("TestTopic", 1))
        when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(false), Future.successful(true))

        when(mockProducer.publish(any[UUID], anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
          .thenReturn(Future.successful(KafkaProducerActor.PublishSuccess))

        val mockStreams = mockKafkaStreams(baseState)

        val actor = testActor(testAggregateId, mockProducer, mockStreams)

        probe.send(actor, PersistentActor.GetState(testAggregateId))
        probe.expectMsg(PersistentActor.StateResponse(Some(baseState)))
      }

      "if up to date check fails" in {
        val probe = TestProbe()

        val testAggregateId = UUID.randomUUID().toString
        val baseState = State(testAggregateId, 3, 3)

        val mockProducer = mock[KafkaProducerActor]
        when(mockProducer.assignedPartition).thenReturn(new TopicPartition("TestTopic", 1))
        when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.failed(new RuntimeException("This is expected")), Future.successful(true))
        when(mockProducer.publish(any[UUID], anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
          .thenReturn(Future.successful(KafkaProducerActor.PublishSuccess))

        val mockStreams = mockKafkaStreams(baseState)

        val actor = testActor(testAggregateId, mockProducer, mockStreams)

        probe.send(actor, PersistentActor.GetState(testAggregateId))
        probe.expectMsg(6.seconds, PersistentActor.StateResponse(Some(baseState)))
      }

      "on a failure to read from the KTable" in {
        val probe = TestProbe()

        val testAggregateId = UUID.randomUUID().toString
        val baseState = State(testAggregateId, 3, 3)

        val mockProducer = defaultMockProducer

        val mockStreams = mock[AggregateStateStoreKafkaStreams]
        when(mockStreams.getAggregateBytes(anyString)).thenReturn(
          Future.failed[Option[Array[Byte]]](new RuntimeException("This is expected")),
          Future.successful(Some(Json.toJson(baseState).toString().getBytes())))

        val actor = testActor(testAggregateId, mockProducer, mockStreams)

        probe.send(actor, PersistentActor.GetState(testAggregateId))
        probe.expectMsg(5.seconds, PersistentActor.StateResponse(Some(baseState)))
      }

      "Return an error and stop the actor on persistent failures" in {
        val expectedException = new RuntimeException("This is expected")
        val testAggregateId = UUID.randomUUID().toString
        val mockProducer = defaultMockProducer
        val mockStreams = mock[AggregateStateStoreKafkaStreams]
        when(mockStreams.getAggregateBytes(anyString)).thenReturn(Future.failed[Option[Array[Byte]]](expectedException))

        val terminationWatcherProbe = TestProbe()

        val probe1 = TestProbe()
        val actor1 = testActor(testAggregateId, mockProducer, mockStreams)
        terminationWatcherProbe.watch(actor1)
        val cmdEnvelope = envelope(Increment(testAggregateId))
        probe1.send(actor1, cmdEnvelope)
        val cmdError1 = probe1.expectMsgType[ACKError](20.seconds)
        cmdError1.exception shouldBe a[AggregateInitializationException]
        terminationWatcherProbe.expectTerminated(actor1)

        val probe2 = TestProbe()
        val actor2 = testActor(testAggregateId, mockProducer, mockStreams)
        terminationWatcherProbe.watch(actor2)
        val applyEvent = PersistentActor.ApplyEvents(testAggregateId, List(CountIncremented(testAggregateId, 1, 1)))
        probe2.send(actor2, applyEvent)
        val cmdError2 = probe2.expectMsgType[ACKError](20.seconds)
        cmdError2.exception shouldBe a[AggregateInitializationException]
        terminationWatcherProbe.expectTerminated(actor2)
      }
    }

    "Not update state if there are no events processed" in {
      val testContext = TestContext.setupDefault
      import testContext._

      val testEnvelope = envelope(DoNothing(testAggregateId))

      probe.send(actor, testEnvelope)
      probe.expectMsg(PersistentActor.ACKSuccess(Some(baseState)))

      verify(mockProducer, never()).publish(any[UUID], anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]])
    }

    "handle exceptions from the domain by returning a AckError" in {
      val testContext = TestContext.setupDefault
      import testContext._

      val testEnvelope: PersistentActor.ProcessMessage[BaseTestCommand] = envelope(FailCommandProcessing(testAggregateId, errorMsg = "failed"))

      probe.send(actor, testEnvelope)
      val commandError = probe.expectMsgClass(classOf[PersistentActor.ACKError])
      // Fuzzy matching because serializing and deserializing gets a different object and messes up .equals even though the two are identical
      commandError.exception.getMessage shouldEqual "failed"

      val testEnvelope2 = envelope(CreateExceptionThrowingEvent(testAggregateId, errorMsg = "failed"))
      probe.send(actor, testEnvelope2)
      val eventError = probe.expectMsgClass(classOf[PersistentActor.ACKError])
      // Fuzzy matching because serializing and deserializing gets a different object and messes up .equals even though the two are identical
      eventError.exception.getMessage shouldEqual "failed"

      val applyEvent = PersistentActor.ApplyEvents(testAggregateId, List(ExceptionThrowingEvent(testAggregateId, 1, errorMsg = "failed")))
      probe.send(actor, applyEvent)
      val applyEventError = probe.expectMsgClass(classOf[PersistentActor.ACKError])
      // Fuzzy matching because serializing and deserializing gets a different object and messes up .equals even though the two are identical
      applyEventError.exception.getMessage shouldEqual "failed"

      val unserializableEventEnvelope = envelope(CreateUnserializableEvent(testAggregateId, errorMsg = "failed"))
      probe.send(actor, unserializableEventEnvelope)
      val unserializableError = probe.expectMsgClass(classOf[PersistentActor.ACKError])
      // Fuzzy matching because serializing and deserializing gets a different object and messes up .equals even though the two are identical
      unserializableError.exception.getMessage shouldEqual "failed"

      // Finally send a command that doesn't produce an error to ensure the actor is left in a state that can handle commands
      val finallySuccessful = envelope(DoNothing(testAggregateId))
      probe.send(actor, finallySuccessful)
      probe.expectMsg(PersistentActor.ACKSuccess(Some(baseState)))
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
      probe.expectMsg(PersistentActor.ACKSuccess(expectedState1))

      producerProbe.expectMsg(Publish(testAggregateId))
      producerProbe.reply(KafkaProducerActor.PublishSuccess)
      probe.expectMsg(PersistentActor.ACKSuccess(expectedState2))
    }

    "Publish events even if they don't update the state" in {
      val producerProbe = TestProbe()
      val testContext = TestContext.setupDefault(mockProducer = probeBackedMockProducer(producerProbe))
      import testContext._

      val probe = TestProbe()

      val testEnvelope = envelope(CreateNoOpEvent(baseState.aggregateId))

      probe.send(actor, testEnvelope)
      producerProbe.expectMsg(Publish(testAggregateId))
      producerProbe.reply(KafkaProducerActor.PublishSuccess)
      probe.expectMsg(PersistentActor.ACKSuccess(Some(baseState)))
    }

    /** TODO add tests for applying multiple events in one applyEvents call */

    "Handle ApplyEvent requests" in {
      val testContext = TestContext.setupDefault
      import testContext._

      val event1 = CountIncremented(baseState.aggregateId, 1, baseState.version + 1)
      val expectedState1: Option[State] = BusinessLogic.handleEvent(Some(baseState), event1)

      val event2 = CountIncremented(expectedState1.get.aggregateId, 1, expectedState1.get.version + 1)
      val expectedState2: Option[State] = BusinessLogic.handleEvent(expectedState1, event2)

      probe.send(actor, PersistentActor.ApplyEvents(testAggregateId, List(event1)))
      probe.send(actor, PersistentActor.ApplyEvents(testAggregateId, List(event2)))

      probe.expectMsg(PersistentActor.ACKSuccess(expectedState1))
      probe.expectMsg(PersistentActor.ACKSuccess(expectedState2))

      verify(mockProducer, times(2)).publish(
        any[UUID],
        any[String],
        any[KafkaProducerActor.MessageToPublish],
        argEquals(Seq[KafkaProducerActor.MessageToPublish]()))
    }

    "Not Crash the actor if publishing eventually succeeds" in {
      val crashingMockProducer = mock[KafkaProducerActor]
      val expectedException = new ExpectedTestException

      when(crashingMockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
      when(crashingMockProducer.messageTracker(any[UUID]))
        .thenReturn(MessageTrackerWithExpiry(MessageTracker.apply(UUID.randomUUID(), 6.seconds), expired = false))
        .thenReturn(MessageTrackerWithExpiry(MessageTracker.apply(UUID.randomUUID(), 6.seconds), expired = false))
        .thenReturn(MessageTrackerWithExpiry(MessageTracker.apply(UUID.randomUUID(), 6.seconds), expired = false))
        .thenReturn(MessageTrackerWithExpiry(MessageTracker.apply(UUID.randomUUID(), 6.seconds), expired = false))
        .thenReturn(MessageTrackerWithExpiry(MessageTracker.alreadyPublished(UUID.randomUUID(), 6.seconds), expired = false))

      when(crashingMockProducer.publish(any[UUID], anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
        .thenReturn(Future.failed(expectedException))
        .thenReturn(Future.failed(expectedException))
        .thenReturn(Future.failed(expectedException))
        .thenReturn(Future.failed(expectedException))
        .thenReturn(Future.successful(PublishSuccess))

      val testContext = TestContext.setupDefault(mockProducer = crashingMockProducer)
      import testContext._

      probe.watch(actor)

      val incrementCmd = Increment(baseState.aggregateId)
      val testEnvelope = envelope(incrementCmd)
      probe.send(actor, testEnvelope)

      probe.expectMsgClass(classOf[PersistentActor.ACKSuccess[Option[State]]])

      // producer should retry publish
      verify(crashingMockProducer, times(5)).publish(
        any[UUID],
        anyString,
        any[KafkaProducerActor.MessageToPublish],
        any[Seq[KafkaProducerActor.MessageToPublish]])
    }

    "Crash the actor to force reinitialization if publishing events times out" in {
      val crashingMockProducer = mock[KafkaProducerActor]
      val expectedException = new ExpectedTestException

      when(crashingMockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
      when(crashingMockProducer.messageTracker(any[UUID]))
        .thenReturn(MessageTrackerWithExpiry(MessageTracker.apply(UUID.randomUUID(), 6.seconds), expired = false))
        .thenReturn(MessageTrackerWithExpiry(MessageTracker.apply(UUID.randomUUID(), 6.seconds), expired = false))
        .thenReturn(MessageTrackerWithExpiry(MessageTracker.apply(UUID.randomUUID(), 6.seconds), expired = false))
        .thenReturn(MessageTrackerWithExpiry(MessageTracker.apply(UUID.randomUUID(), 6.seconds), expired = false))
        .thenReturn(MessageTrackerWithExpiry(MessageTracker.apply(UUID.randomUUID(), 6.seconds), expired = true))

      when(crashingMockProducer.publish(any[UUID], anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
        .thenReturn(Future.failed(expectedException))

      val testContext = TestContext.setupDefault(mockProducer = crashingMockProducer)
      import testContext._

      probe.watch(actor)

      val incrementCmd = Increment(baseState.aggregateId)
      val testEnvelope = envelope(incrementCmd)
      probe.send(actor, testEnvelope)

      probe.expectMsgClass(classOf[ACKError])
      probe.expectTerminated(actor)

      // producer should retry publish
      verify(crashingMockProducer, times(5)).publish(
        any[UUID],
        anyString,
        any[KafkaProducerActor.MessageToPublish],
        any[Seq[KafkaProducerActor.MessageToPublish]])
    }

    "Wrap and return the error from publishing to Kafka if publishing explicitly fails consistently" in {
      val failingMockProducer = mock[KafkaProducerActor]
      val expectedException = new ExpectedTestException
      when(failingMockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
      when(failingMockProducer.publish(any[UUID], anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
        .thenReturn(Future.successful(KafkaProducerActor.PublishFailure(expectedException)))

      val testContext = TestContext.setupDefault(mockProducer = failingMockProducer)
      import testContext._

      probe.watch(actor)

      val incrementCmd = Increment(baseState.aggregateId)
      val testEnvelope = envelope(incrementCmd)
      probe.send(actor, testEnvelope)
      probe.expectMsgClass(classOf[ACKError])
      probe.expectTerminated(actor)
    }

    "Retry publishing to Kafka if publishing explicitly fails" in {
      val failingMockProducer = mock[KafkaProducerActor]
      val expectedException = new ExpectedTestException
      when(failingMockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
      when(failingMockProducer.publish(any[UUID], anyString, any[KafkaProducerActor.MessageToPublish], any[Seq[KafkaProducerActor.MessageToPublish]]))
        .thenReturn(Future.successful(KafkaProducerActor.PublishFailure(expectedException)))
        .thenReturn(Future.successful(KafkaProducerActor.PublishSuccess))

      val testContext = TestContext.setupDefault(mockProducer = failingMockProducer)
      import testContext._

      val incrementCmd = Increment(baseState.aggregateId)
      val testEnvelope = envelope(incrementCmd)
      val expectedEvent = CountIncremented(baseState.aggregateId, 1, baseState.version + 1)
      val expectedState = BusinessLogic.handleEvent(Some(baseState), expectedEvent)

      probe.send(actor, testEnvelope)
      probe.expectMsg(PersistentActor.ACKSuccess(expectedState))
    }

    "Be able to correctly extract the correct aggregate ID from messages" in {
      val command1 = PersistentActor.ProcessMessage(aggregateId = "foobarbaz", message = "unused")
      val command2 = PersistentActor.ProcessMessage(aggregateId = randomUUID, message = "unused")

      val getState1 = PersistentActor.GetState(aggregateId = "foobarbaz")
      val getState2 = PersistentActor.GetState(aggregateId = randomUUID)

      val command3 = PersistentActor.ApplyEvents(aggregateId = "testAggregateId", events = List("unused"))
      val command4 = PersistentActor.ApplyEvents(aggregateId = randomUUID, events = List("unused"))

      RoutableMessage.extractEntityId(command1) shouldEqual command1.aggregateId
      RoutableMessage.extractEntityId(command2) shouldEqual command2.aggregateId

      RoutableMessage.extractEntityId(getState1) shouldEqual getState1.aggregateId
      RoutableMessage.extractEntityId(getState2) shouldEqual getState2.aggregateId

      RoutableMessage.extractEntityId(command3) shouldEqual command3.aggregateId
      RoutableMessage.extractEntityId(command4) shouldEqual command4.aggregateId
    }

    "Passivate after the actor idle timeout threshold is exceeded" in {
      val testContext = TestContext.setupDefault
      import testContext._
      processIncrementCommand(actor, baseState, mockProducer)
      probe.watch(actor)
      actor ! ReceiveTimeout
      probe.expectMsg(Passivate(Stop))
      probe.reply(Stop)
      probe.expectTerminated(actor)
    }

    // Sometimes we need to compare toString values of AnyRef, e.g. for exceptions
    def doSerde(envelope: AnyRef, shouldCompareStringResults: Boolean = false): Unit = {
      val serialization = SerializationExtension.get(system)
      val serializer = serialization.findSerializerFor(envelope)
      val serialized = serialization.serialize(envelope).get
      val manifest = Serializers.manifestFor(serializer, envelope)
      val deserialized = serialization.deserialize(serialized, serializer.identifier, manifest).get
      if (shouldCompareStringResults) {
        deserialized.toString shouldEqual envelope.toString
      } else {
        deserialized shouldEqual envelope
      }
    }

    "Serialize/Deserialize a CommandEnvelope from Akka" in {
      doSerde(PersistentActor.ProcessMessage[String]("hello", "test2"))
      doSerde(envelope(Increment(UUID.randomUUID().toString)))
    }

    "Serialize/Deserialize an ApplyEvent from Akka" in {
      import akka.serialization.SerializationExtension

      def doSerde[A](envelope: ApplyEvents[A]): Unit = {
        val serialization = SerializationExtension.get(system)
        val serializer = serialization.findSerializerFor(envelope)
        val serialized = serialization.serialize(envelope).get
        val manifest = Serializers.manifestFor(serializer, envelope)
        val deserialized = serialization.deserialize(serialized, serializer.identifier, manifest).get
        deserialized shouldEqual envelope
      }

      doSerde(PersistentActor.ApplyEvents[String](UUID.randomUUID().toString, List("test2")))
      doSerde(eventEnvelope(CountIncremented(UUID.randomUUID().toString, 1, 1)))
    }

    "Serialize/Deserialize response types from Akka" in {
      doSerde(PersistentActor.StateResponse[String](Some("test state")))
      doSerde(PersistentActor.StateResponse[String](None))
      doSerde(PersistentActor.StateResponse[State](Some(State(UUID.randomUUID().toString, 100, 3))))
      doSerde(PersistentActor.StateResponse[State](None))

      doSerde(PersistentActor.ACKSuccess[String](Some("success!")))
      doSerde(PersistentActor.ACKSuccess[String](None))
      doSerde(PersistentActor.ACKSuccess[State](Some(State(UUID.randomUUID().toString, 10, 3))))
      doSerde(PersistentActor.ACKSuccess[State](None))

      doSerde(PersistentActor.ACKError(new ExpectedTestException), shouldCompareStringResults = true)
      doSerde(PersistentActor.ACKError(new Throwable(new RuntimeException)), shouldCompareStringResults = true)

      def exceptionAsThrowable(cause: Throwable): Throwable = cause
      doSerde(PersistentActor.ACKError(exceptionAsThrowable(new ExpectedTestException)), shouldCompareStringResults = true)
      doSerde(
        PersistentActor.ACKError(exceptionAsThrowable(KafkaPublishTimeoutException("some-aggregate-id", new Throwable("error")))),
        shouldCompareStringResults = true)

      doSerde(
        PersistentActor.ACKError(exceptionAsThrowable(AggregateInitializationException("some-aggregate-id", new Throwable("error")))),
        shouldCompareStringResults = true)
    }
  }
}

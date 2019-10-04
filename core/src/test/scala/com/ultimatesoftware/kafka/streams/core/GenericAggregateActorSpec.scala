// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, KafkaPartitionMetadata, KafkaStreamsKeyValueStore, StatePlusPartitionMetadata }
import com.ultimatesoftware.scala.core.messaging.{ DefaultCommandMetadata, EventMessage, StateMessage }
import com.ultimatesoftware.scala.core.monitoring.metrics.NoOpMetricsProvider
import com.ultimatesoftware.scala.core.utils
import com.ultimatesoftware.scala.core.utils.BlockchainChecksum
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.{ BeforeAndAfterAll, Matchers, PartialFunctionValues, WordSpecLike }
import org.scalatestplus.mockito.MockitoSugar
import play.api.libs.json.{ Format, JsValue, Json }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class GenericAggregateActorSpec extends TestKit(ActorSystem("GenericAggregateActorSpec")) with WordSpecLike with Matchers
  with BeforeAndAfterAll with MockitoSugar with TestBoundedContext with PartialFunctionValues {

  private implicit val timeout: Timeout = Timeout(10.seconds)
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private def testActor(aggregateId: UUID = UUID.randomUUID(), producerActor: KafkaProducerActor[State, UUID, BaseTestEvent],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams): ActorRef = {

    val metrics = GenericAggregateActor.createMetrics(NoOpMetricsProvider, "testAggregate")

    val props = GenericAggregateActor.props(aggregateId, BusinessLogic, producerActor, metrics, aggregateKafkaStreamsImpl)

    system.actorOf(props)
  }

  private def mockProps(cmd: BaseTestCommand): DefaultCommandMetadata = {
    DefaultCommandMetadata(id = cmd.aggregateId, userId = UUID.randomUUID(), sessionId = UUID.randomUUID(),
      tenantId = None, aggregateId = None, expectedSequenceNumber = 0,
      timestamp = Instant.now.truncatedTo(ChronoUnit.SECONDS), effectiveDateTime = Instant.now.truncatedTo(ChronoUnit.SECONDS),
      correlationId = None, user = None, metadata = Map.empty)
  }
  private def envelope(cmd: BaseTestCommand): GenericAggregateActor.CommandEnvelope[UUID, BaseTestCommand, DefaultCommandMetadata] = {
    GenericAggregateActor.CommandEnvelope(cmd.aggregateId, mockProps(cmd), cmd)
  }

  private def mockAggregateKeyValueStore(contents: Map[String, StatePlusPartitionMetadata]): KafkaStreamsKeyValueStore[String, StatePlusPartitionMetadata] = {
    val mockStore = mock[KafkaStreamsKeyValueStore[String, StatePlusPartitionMetadata]]
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

  "GenericAggregateActor" should {
    "Properly initialize from Kafka streams" in {
      val probe = TestProbe()

      val testAggregateId = UUID.randomUUID()
      val currentSequenceNumber = 3
      val baseState = State(testAggregateId, 3, currentSequenceNumber, Instant.now)

      val mockProducer = mock[KafkaProducerActor[State, UUID, BaseTestEvent]]
      when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
      when(mockProducer.publish(any[UUID], any[Seq[StateMessage[JsValue]]], any[Seq[EventMessage[BaseTestEvent]]])).thenReturn(Future.successful(Done))

      val incrementCmd = Increment(testAggregateId)
      val testEnvelope = envelope(incrementCmd)

      val initialStateMessage = StateMessage(
        fullIdentifier = testAggregateId.toString, aggregateId = testAggregateId.toString,
        tenantId = testEnvelope.meta.tenantId.getOrElse(utils.EmptyUUID), body = Some(Json.toJson(baseState)),
        eventSequenceNumber = currentSequenceNumber, checksum = Some("thisIsTheStateChecksum"),
        `type` = classOf[State].getName, schemaVersion = "1.0")

      val testAggregateStateMeta = StatePlusPartitionMetadata(initialStateMessage, KafkaPartitionMetadata(topic = "testTopic", partition = 0, offset = 100, key = testAggregateId.toString))
      val mockStateStore = mockAggregateKeyValueStore(Map(testAggregateId.toString -> testAggregateStateMeta))
      val mockKafkaStreams = mock[AggregateStateStoreKafkaStreams]
      when(mockKafkaStreams.aggregateQueryableStateStore).thenReturn(mockStateStore)
      when(mockKafkaStreams.substatesForAggregate(anyString)(any[ExecutionContext])).thenCallRealMethod

      val actor = testActor(testAggregateId, mockProducer, mockKafkaStreams)

      val expectedEvent = CountIncremented(testAggregateId, 1, currentSequenceNumber + 1, testEnvelope.meta.timestamp)
      probe.send(actor, testEnvelope)

      val expectedEventMessage = EventMessage.create[BaseTestEvent](
        testEnvelope.meta.toEventProperties.withSequenceNumber(currentSequenceNumber + 1),
        expectedEvent)
      val expectedState = BusinessLogic.processEvent(Some(baseState), expectedEventMessage)
      val expectedResource = BusinessLogic.resourceFromAggregate(expectedState)

      probe.expectMsg(GenericAggregateActor.CommandSuccess(expectedState))

      val expectedStateJson = expectedState.map(s ⇒ Json.toJson(s))

      val newState = initialStateMessage.copy(
        body = expectedStateJson,
        eventSequenceNumber = initialStateMessage.eventSequenceNumber + 1,
        checksum = Some(BlockchainChecksum.calculateChecksum(expectedStateJson, initialStateMessage.checksum)))

      verify(mockProducer).publish(testAggregateId, Seq(newState), Seq(expectedEventMessage))
    }

    "Be able to correctly extract the correct aggregate ID from messages" in {
      val command1 = GenericAggregateActor.CommandEnvelope(aggregateId = "foobarbaz", meta = DefaultCommandMetadata.empty(), command = "unused")
      val command2 = GenericAggregateActor.CommandEnvelope(aggregateId = UUID.randomUUID(), meta = DefaultCommandMetadata.empty(), command = "unused")

      val getState1 = GenericAggregateActor.GetState(aggregateId = "foobarbaz")
      val getState2 = GenericAggregateActor.GetState(aggregateId = UUID.randomUUID())

      GenericAggregateActor.RoutableMessage.extractEntityId[String](command1) shouldEqual command1.aggregateId
      GenericAggregateActor.RoutableMessage.extractEntityId[UUID](command2) shouldEqual command2.aggregateId

      GenericAggregateActor.RoutableMessage.extractEntityId[String](getState1) shouldEqual getState1.aggregateId
      GenericAggregateActor.RoutableMessage.extractEntityId[UUID](getState2) shouldEqual getState2.aggregateId
    }
  }
}

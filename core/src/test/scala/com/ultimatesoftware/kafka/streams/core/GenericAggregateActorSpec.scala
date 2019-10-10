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
import com.ultimatesoftware.scala.core.monitoring.metrics.NoOpMetricsProvider
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

  private def testActor(aggregateId: UUID = UUID.randomUUID(), producerActor: KafkaProducerActor[UUID, State, BaseTestEvent],
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue]): ActorRef = {

    val metrics = GenericAggregateActor.createMetrics(NoOpMetricsProvider, "testAggregate")

    val props = GenericAggregateActor.props(aggregateId, kafkaStreamsLogic, producerActor, metrics, aggregateKafkaStreamsImpl)

    system.actorOf(props)
  }

  private def envelope(cmd: BaseTestCommand): GenericAggregateActor.CommandEnvelope[UUID, BaseTestCommand, TimestampMeta] = {
    GenericAggregateActor.CommandEnvelope(cmd.aggregateId, TimestampMeta(Instant.now.truncatedTo(ChronoUnit.SECONDS)), cmd)
  }

  private def mockAggregateKeyValueStore(
    contents: Map[String, StatePlusPartitionMetadata[JsValue]]): KafkaStreamsKeyValueStore[String, StatePlusPartitionMetadata[JsValue]] = {
    val mockStore = mock[KafkaStreamsKeyValueStore[String, StatePlusPartitionMetadata[JsValue]]]
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

      val mockProducer = mock[KafkaProducerActor[UUID, State, BaseTestEvent]]
      when(mockProducer.isAggregateStateCurrent(anyString)).thenReturn(Future.successful(true))
      when(mockProducer.publish(any[UUID], any[Seq[(String, JsValue)]], any[Seq[(String, BaseTestEvent)]])).thenReturn(Future.successful(Done))

      val incrementCmd = Increment(testAggregateId)
      val testEnvelope = envelope(incrementCmd)

      val testAggregateStateMeta = StatePlusPartitionMetadata(
        Json.toJson(baseState),
        KafkaPartitionMetadata(topic = "testTopic", partition = 0, offset = 100, key = testAggregateId.toString))
      val mockStateStore = mockAggregateKeyValueStore(Map(testAggregateId.toString -> testAggregateStateMeta))
      val mockKafkaStreams = mock[AggregateStateStoreKafkaStreams[JsValue]]
      when(mockKafkaStreams.aggregateQueryableStateStore).thenReturn(mockStateStore)
      when(mockKafkaStreams.substatesForAggregate(anyString)(any[ExecutionContext])).thenCallRealMethod

      val actor = testActor(testAggregateId, mockProducer, mockKafkaStreams)

      val expectedEvent = CountIncremented(testAggregateId, 1, currentSequenceNumber + 1, testEnvelope.meta.timestamp)
      probe.send(actor, testEnvelope)

      val expectedState = BusinessLogic.handleEvent(Some(baseState), expectedEvent, testEnvelope.meta)

      probe.expectMsg(GenericAggregateActor.CommandSuccess(expectedState))

      val expectedStateKeyValues = expectedState.toSeq.flatMap { state ⇒
        val states = kafkaStreamsLogic.aggregateComposer.decompose(testAggregateId, state).toSeq
        states.map(s ⇒ BusinessLogic.stateKeyExtractor(s) -> s)
      }

      val expectedEventKeyVal = s"${expectedEvent.aggregateId}:${expectedEvent.sequenceNumber}" -> expectedEvent
      verify(mockProducer).publish(testAggregateId, expectedStateKeyValues, Seq(expectedEventKeyVal))
    }

    "Be able to correctly extract the correct aggregate ID from messages" in {
      val command1 = GenericAggregateActor.CommandEnvelope(aggregateId = "foobarbaz", meta = TimestampMeta(Instant.now), command = "unused")
      val command2 = GenericAggregateActor.CommandEnvelope(aggregateId = UUID.randomUUID(), meta = TimestampMeta(Instant.now), command = "unused")

      val getState1 = GenericAggregateActor.GetState(aggregateId = "foobarbaz")
      val getState2 = GenericAggregateActor.GetState(aggregateId = UUID.randomUUID())

      GenericAggregateActor.RoutableMessage.extractEntityId[String](command1) shouldEqual command1.aggregateId
      GenericAggregateActor.RoutableMessage.extractEntityId[UUID](command2) shouldEqual command2.aggregateId

      GenericAggregateActor.RoutableMessage.extractEntityId[String](getState1) shouldEqual getState1.aggregateId
      GenericAggregateActor.RoutableMessage.extractEntityId[UUID](getState2) shouldEqual getState2.aggregateId
    }
  }
}

// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import akka.pattern.ask
import akka.Done
import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.testkit.{ TestKit, TestProbe }
import com.ultimatesoftware.kafka.streams.{ GlobalKTableMetadataHandler, KafkaPartitionMetadata, KafkaStreamsKeyValueStore }
import com.ultimatesoftware.kafka.streams.core.KafkaProducerActorImpl.AggregateStateRates
import com.ultimatesoftware.scala.core.kafka.{ KafkaBytesProducer, KafkaRecordMetadata }
import com.ultimatesoftware.scala.core.monitoring.metrics.NoOpMetricsProvider
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.apache.kafka.common.TopicPartition
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.ExecutionContext.Implicits.global

class KafkaProducerActorSpec extends TestKit(ActorSystem("KafkaProducerActorSpec")) with AnyWordSpecLike with Matchers
  with TestBoundedContext with MockitoSugar {

  private def createRecordMeta(topic: String, partition: Int, offset: Int): RecordMetadata = {
    new RecordMetadata(
      new TopicPartition(topic, partition), 0, offset, Instant.now.toEpochMilli,
      0L, 0, 0)
  }

  private def testProducerActor(assignedPartition: TopicPartition, mockProducer: KafkaBytesProducer): ActorRef = {
    val futureOffset = 100L // Mock for internal metadata handler just returns a high number so that the partition always appears up to date when initializing
    val mockMetaHandler = mock[GlobalKTableMetadataHandler]
    val mockQueryableStore = mock[KafkaStreamsKeyValueStore[String, KafkaPartitionMetadata]]
    when(mockQueryableStore.get(anyString))
      .thenReturn(Future.successful(Some(KafkaPartitionMetadata(assignedPartition.topic(), assignedPartition.partition(), futureOffset, ""))))
    when(mockMetaHandler.stateMetaQueryableStore).thenReturn(mockQueryableStore)
    system.actorOf(Props(new KafkaProducerActorImpl(assignedPartition, NoOpMetricsProvider, mockMetaHandler, kafkaStreamsLogic, Some(mockProducer))))
  }

  private def mockRecordMetadata(assignedPartition: TopicPartition): KafkaRecordMetadata[String] = {
    val recordMeta = createRecordMeta(assignedPartition.topic(), assignedPartition.partition(), 1)
    new KafkaRecordMetadata[String](None, recordMeta)
  }

  private def testObjects(strings: Seq[String]): Seq[(String, Array[Byte])] = {
    strings.map(str ⇒ str -> str.getBytes())
  }
  private def records(events: Seq[(String, Array[Byte])], states: Seq[(String, Array[Byte])]): Seq[ProducerRecord[String, Array[Byte]]] = {
    val eventRecords = events.map { tup ⇒
      new ProducerRecord(kafkaStreamsLogic.kafka.eventsTopic.name, tup._1, tup._2)
    }
    val stateRecords = states.map { tup ⇒
      new ProducerRecord(kafkaStreamsLogic.kafka.stateTopic.name, tup._1, tup._2)
    }

    eventRecords ++ stateRecords
  }

  private def setupTransactions(mockProducer: KafkaBytesProducer): Unit = {
    when(mockProducer.initTransactions()(any[ExecutionContext])).thenReturn(Future.unit)
    doNothing().when(mockProducer).beginTransaction()
    doNothing().when(mockProducer).abortTransaction()
    doNothing().when(mockProducer).commitTransaction()
  }

  "KafkaProducerActor" should {
    val testEvents1 = testObjects(Seq("event1", "event2", "event3"))
    val testAggs1 = testObjects(Seq("event1", "event2", "event3"))
    val testEvents2 = testObjects(Seq("event3", "event4"))
    val testAggs2 = testObjects(Seq("agg3", "agg3"))

    "Recovers from beginTransaction failure" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[KafkaBytesProducer]
      val mockMetadata = mockRecordMetadata(assignedPartition)
      when(mockProducer.initTransactions()(any[ExecutionContext])).thenReturn(Future.successful())
      when(mockProducer.putRecord(any[ProducerRecord[String, Array[Byte]]])).thenReturn(Future.successful(mockMetadata))
      // Fail first transaction and then succeed always
      doThrow(new IllegalStateException("This is expected")).doNothing().when(mockProducer).beginTransaction()
      doNothing().when(mockProducer).abortTransaction()
      doNothing().when(mockProducer).commitTransaction()

      when(mockProducer.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]]))
        .thenReturn(Seq(Future.successful(mockMetadata)))
      when(mockProducer.putRecord(any[ProducerRecord[String, Array[Byte]]]))
        .thenReturn(Future.successful(mockMetadata))

      val actor = testProducerActor(assignedPartition, mockProducer)
      // First time beginTransaction will fail and commitTransaction won't be executed
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(actor, KafkaProducerActorImpl.FlushMessages)
      awaitAssert({
        verify(mockProducer, times(1)).beginTransaction()
      }, 3 seconds, 1 seconds)
      verify(mockProducer, times(0)).commitTransaction()
      // Actor should recover from begin transaction error and successfully commit
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(actor, KafkaProducerActorImpl.FlushMessages)
      awaitAssert({
        verify(mockProducer, times(2)).beginTransaction()
        verify(mockProducer, times(1)).commitTransaction()
      }, 3 seconds, 1 seconds)
    }

    "Gets to initialize the state if initializing kafka transactions fails" in {
      TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[KafkaBytesProducer]
      val mockMetadata = mockRecordMetadata(assignedPartition)

      when(mockProducer.initTransactions()(any[ExecutionContext]))
        .thenReturn(Future.failed(new IllegalStateException("This is expected")))
        .thenReturn(Future.successful())
      when(mockProducer.putRecord(any[ProducerRecord[String, Array[Byte]]]))
        .thenReturn(Future.successful(mockMetadata))

      testProducerActor(assignedPartition, mockProducer)
      awaitAssert({
        // tries to initTransactions twice, first time fails
        verify(mockProducer, times(2)).initTransactions()(any[ExecutionContext])
        // second time is a success and it calls initializeState only once, what cause a call to putRecord
        verify(mockProducer, times(1)).putRecord(any[ProducerRecord[String, Array[Byte]]])
      }, 11 seconds, 10 second)
    }

    "Retry initialization if the flush record fails to write to Kafka" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducerFailsPutRecord = mock[KafkaBytesProducer]
      val failingFlush = testProducerActor(assignedPartition, mockProducerFailsPutRecord)

      val mockMetadata = mockRecordMetadata(assignedPartition)
      setupTransactions(mockProducerFailsPutRecord)
      when(mockProducerFailsPutRecord.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]]))
        .thenReturn(Seq(Future.successful(mockMetadata)))
      when(mockProducerFailsPutRecord.putRecord(any[ProducerRecord[String, Array[Byte]]]))
        .thenReturn(Future.failed(new RuntimeException("This is expected")), Future.successful(mockMetadata))

      probe.send(failingFlush, KafkaProducerActorImpl.Publish(testAggs2, testEvents2))
      probe.send(failingFlush, KafkaProducerActorImpl.FlushMessages)
      probe.expectMsg(6.seconds, Done) // Need to wait a little extra since failing to write the flush record will wait 3 seconds before retrying

      verify(mockProducerFailsPutRecord, times(2)).putRecord(any[ProducerRecord[String, Array[Byte]]])
      verify(mockProducerFailsPutRecord).putRecords(records(testEvents2, testAggs2))
      verify(mockProducerFailsPutRecord).commitTransaction()
    }

    "Stash IsAggregateStateCurrent messages until fully initialized when no messages inflight" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[KafkaBytesProducer]
      val mockMetadata = mockRecordMetadata(assignedPartition)
      setupTransactions(mockProducer)
      when(mockProducer.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]]))
        .thenReturn(Seq(Future.successful(mockMetadata)))
      when(mockProducer.putRecord(any[ProducerRecord[String, Array[Byte]]]))
        .thenReturn(Future.successful(mockMetadata))
      val actor = testProducerActor(assignedPartition, mockProducer)
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      // Send IsAggregateStateCurrent messages to stash
      val isAggregateStateCurrent = KafkaProducerActorImpl.IsAggregateStateCurrent("bar", Instant.now.plusSeconds(10L))
      probe.send(actor, isAggregateStateCurrent)
      // Verify that we haven't initialized transactions yet so we are in the uninitialized state and messages were stashed
      verify(mockProducer, times(0)).initTransactions()(any[ExecutionContext])
      val response = actor.ask(isAggregateStateCurrent)(10 seconds).map(Some(_)).recoverWith { case _ ⇒ Future.successful(None) }
      assert(Await.result(response, 10 seconds).isDefined)
    }

    "Answer to IsAggregateStateCurrent when messages in flight" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[KafkaBytesProducer]
      val mockMetadata = mockRecordMetadata(assignedPartition)
      setupTransactions(mockProducer)
      when(mockProducer.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]]))
        .thenReturn(Seq(Future.successful(mockMetadata)))
      when(mockProducer.putRecord(any[ProducerRecord[String, Array[Byte]]]))
        .thenReturn(Future.successful(mockMetadata))
      val actor = testProducerActor(assignedPartition, mockProducer)
      // Send IsAggregateStateCurrent messages to stash
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      // Wait until published message is committed to be sure we are processing
      awaitAssert({
        verify(mockProducer, times(1)).commitTransaction()
      }, 10 seconds, 1 seconds)

      val barRecord1 = KafkaRecordMetadata(Some("bar"), createRecordMeta("testTopic", 0, 0))
      probe.send(actor, KafkaProducerActorImpl.EventsPublished(Seq(probe.ref), Seq(barRecord1)))
      val isAggregateStateCurrent = KafkaProducerActorImpl.IsAggregateStateCurrent("bar", Instant.now.plusSeconds(10L))
      val response = actor.ask(isAggregateStateCurrent)(10 seconds).map(Some(_)).recoverWith { case _ ⇒ Future.successful(None) }
      assert(Await.result(response, 10 seconds).isDefined)
    }

    "Stash Publish messages and publish them when fully initialized" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[KafkaBytesProducer]
      val mockMetadata = mockRecordMetadata(assignedPartition)
      setupTransactions(mockProducer)
      when(mockProducer.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]]))
        .thenReturn(Seq(Future.successful(mockMetadata)))
      when(mockProducer.putRecord(any[ProducerRecord[String, Array[Byte]]]))
        .thenReturn(Future.successful(mockMetadata))
      val actor = testProducerActor(assignedPartition, mockProducer)
      // Send publish messages to stash
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      // Verify that we haven't initialized transactions yet so we are in the uninitialized state and messages were stashed
      verify(mockProducer, times(0)).initTransactions()(any[ExecutionContext])
      // Wait until the stashed messages gets committed
      awaitAssert({
        verify(mockProducer, times(1)).commitTransaction()
      }, 10 seconds, 1 seconds)
    }

    "Try to publish new incoming messages to Kafka if publishing to Kafka fails" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducerFailsPutRecords = mock[KafkaBytesProducer]
      val failingPut = testProducerActor(assignedPartition, mockProducerFailsPutRecords)

      val mockMetadata = mockRecordMetadata(assignedPartition)
      setupTransactions(mockProducerFailsPutRecords)
      when(mockProducerFailsPutRecords.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]]))
        .thenReturn(Seq(Future.failed(new RuntimeException("This is expected"))), Seq(Future.successful(mockMetadata)))
      when(mockProducerFailsPutRecords.putRecord(any[ProducerRecord[String, Array[Byte]]]))
        .thenReturn(Future.successful(mockMetadata))

      probe.send(failingPut, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(failingPut, KafkaProducerActorImpl.FlushMessages)
      probe.expectNoMessage()
      verify(mockProducerFailsPutRecords).beginTransaction()
      verify(mockProducerFailsPutRecords).putRecords(records(testEvents1, testAggs1))
      verify(mockProducerFailsPutRecords).abortTransaction()

      probe.send(failingPut, KafkaProducerActorImpl.Publish(testAggs2, testEvents2))
      probe.send(failingPut, KafkaProducerActorImpl.FlushMessages)
      probe.expectMsg(Done)
      verify(mockProducerFailsPutRecords).putRecords(records(testEvents2, testAggs2))
      verify(mockProducerFailsPutRecords).commitTransaction()
    }

    "Try to publish new incoming messages to Kafka if committing to Kafka fails" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducerFailsCommit = mock[KafkaBytesProducer]
      val failingCommit = testProducerActor(assignedPartition, mockProducerFailsCommit)

      when(mockProducerFailsCommit.initTransactions()(any[ExecutionContext])).thenReturn(Future.unit)
      doNothing().when(mockProducerFailsCommit).beginTransaction()
      doNothing().when(mockProducerFailsCommit).abortTransaction()
      doThrow(new RuntimeException("This is expected")).when(mockProducerFailsCommit).commitTransaction()

      val mockMetadata = mockRecordMetadata(assignedPartition)
      when(mockProducerFailsCommit.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]]))
        .thenReturn(Seq(Future.successful(mockMetadata)))
      when(mockProducerFailsCommit.putRecord(any[ProducerRecord[String, Array[Byte]]]))
        .thenReturn(Future.successful(mockMetadata))

      probe.send(failingCommit, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(failingCommit, KafkaProducerActorImpl.FlushMessages)
      probe.expectNoMessage()
      verify(mockProducerFailsCommit).beginTransaction()
      verify(mockProducerFailsCommit).putRecords(records(testEvents1, testAggs1))
      verify(mockProducerFailsCommit).commitTransaction()
      verify(mockProducerFailsCommit).abortTransaction()

      // Since we can't stub the void method to throw an exception and then succeed, we just care that the actor attempts to send the next set of records
      probe.send(failingCommit, KafkaProducerActorImpl.Publish(testAggs2, testEvents2))
      probe.send(failingCommit, KafkaProducerActorImpl.FlushMessages)
      probe.expectNoMessage()
      verify(mockProducerFailsCommit).putRecords(records(testEvents2, testAggs2))
    }
  }

  "KafkaProducerActorState" should {
    implicit val unusedStateParent: ActorRef = TestProbe().ref
    implicit val rates: KafkaProducerActorImpl.AggregateStateRates = AggregateStateRates(
      NoOpMetricsProvider.createRate("aggregateCurrentRateUnused"),
      NoOpMetricsProvider.createRate("aggregateNotCurrentRateUnused"))

    val fooRecord1 = KafkaRecordMetadata(Some("foo"), createRecordMeta("testTopic", 0, 1))
    val barRecord1 = KafkaRecordMetadata(Some("bar"), createRecordMeta("testTopic", 0, 2))
    val bazRecord1 = KafkaRecordMetadata(Some("baz"), createRecordMeta("testTopic", 0, 3))
    val fooRecord2 = KafkaRecordMetadata(Some("foo"), createRecordMeta("testTopic", 0, 4))
    val barRecord2 = KafkaRecordMetadata(Some("bar"), createRecordMeta("testTopic", 0, 5))

    val exampleMetadata: Seq[KafkaRecordMetadata[String]] = Seq(fooRecord1, barRecord1, bazRecord1, fooRecord2)

    "Record new record metadata marked as in flight" in {
      val empty = KafkaProducerActorState.empty

      val newState = empty.addInFlight(exampleMetadata)
      newState.inFlight should contain allElementsOf Seq(barRecord1, bazRecord1, fooRecord2)

      newState.inFlightForAggregate("foo") should contain only fooRecord2
      newState.inFlightForAggregate("bar") should contain only barRecord1
      newState.inFlightForAggregate("baz") should contain only bazRecord1
      newState.inFlightForAggregate("missing") should have length 0

      val newState2 = newState.addInFlight(Seq(barRecord2))
      newState2.inFlightForAggregate("bar") should contain only barRecord2
    }

    "Track buffered pending writes" in {
      val empty = KafkaProducerActorState.empty

      val sender = TestProbe().ref
      val publishMsg = KafkaProducerActorImpl.Publish(Seq("foo" -> "foo".getBytes()), Seq("foo" -> "foo".getBytes()))

      val newState = empty.addPendingWrites(sender, publishMsg)
      newState.pendingWrites should contain only KafkaProducerActorImpl.PublishWithSender(sender, publishMsg)

      val flushedState = newState.flushWrites()
      flushedState.pendingWrites should have length 0
    }

    "Track pending aggregate initializations" in {
      val empty = KafkaProducerActorState.empty

      val upToDateProbe = TestProbe()
      val isStateCurrentMsg = KafkaProducerActorImpl.IsAggregateStateCurrent("bar", Instant.now.plusSeconds(10L))
      val expiredProbe = TestProbe()
      val isStateCurrentMsg2 = KafkaProducerActorImpl.IsAggregateStateCurrent("baz", Instant.now.minusSeconds(1L))
      val stillWaitingProbe = TestProbe()
      val isStateCurrentMsg3 = KafkaProducerActorImpl.IsAggregateStateCurrent("foo", Instant.now.plusSeconds(10L))

      val newState = empty.addInFlight(exampleMetadata).addPendingInitialization(upToDateProbe.ref, isStateCurrentMsg)
        .addPendingInitialization(expiredProbe.ref, isStateCurrentMsg2)
        .addPendingInitialization(stillWaitingProbe.ref, isStateCurrentMsg3)

      val expectedPendingInit = KafkaProducerActorImpl.PendingInitialization(upToDateProbe.ref, isStateCurrentMsg.aggregateId,
        isStateCurrentMsg.expirationTime)
      val expectedPendingInit2 = KafkaProducerActorImpl.PendingInitialization(expiredProbe.ref, isStateCurrentMsg2.aggregateId,
        isStateCurrentMsg2.expirationTime)
      val expectedPendingInit3 = KafkaProducerActorImpl.PendingInitialization(stillWaitingProbe.ref, isStateCurrentMsg3.aggregateId,
        isStateCurrentMsg3.expirationTime)
      newState.pendingInitializations should contain allElementsOf Seq(expectedPendingInit, expectedPendingInit2, expectedPendingInit3)

      val barRecordPartitionMeta = KafkaPartitionMetadata(
        topic = barRecord1.wrapped.topic(),
        partition = barRecord1.wrapped.partition(), offset = barRecord1.wrapped.offset(), key = "bar")
      val processedState = newState.processedUpTo(barRecordPartitionMeta)
      processedState.pendingInitializations should contain only expectedPendingInit3

      upToDateProbe.expectMsg(true)
      expiredProbe.expectMsg(false)
      stillWaitingProbe.expectNoMessage()
    }
  }
}

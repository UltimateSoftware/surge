// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ TestKit, TestProbe }
import com.ultimatesoftware.kafka.streams.KafkaPartitionMetadata
import com.ultimatesoftware.kafka.streams.core.KafkaProducerActorImpl.AggregateStateRates
import com.ultimatesoftware.scala.core.kafka.KafkaRecordMetadata
import com.ultimatesoftware.scala.core.monitoring.metrics.NoOpMetricsProvider
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.{ Matchers, WordSpecLike }

class KafkaProducerActorSpec extends TestKit(ActorSystem("KafkaProducerActorSpec")) with WordSpecLike with Matchers {

  private def createRecordMeta(topic: String, partition: Int, offset: Int) = {
    new RecordMetadata(
      new TopicPartition(topic, partition), 0, offset, Instant.now.toEpochMilli,
      0L, 0, 0)
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

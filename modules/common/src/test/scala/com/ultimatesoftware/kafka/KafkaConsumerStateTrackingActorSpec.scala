// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ TestKit, TestProbe }
import com.ultimatesoftware.scala.core.kafka.{ HostPort, PartitionAssignments }
import org.apache.kafka.common.TopicPartition
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class KafkaConsumerStateTrackingActorSpec extends TestKit(ActorSystem("KafkaConsumerStateTrackingActorSpec")) with AnyWordSpecLike with Matchers {
  import KafkaConsumerStateTrackingActor._

  private val initialState = Map[HostPort, List[TopicPartition]](
    HostPort("foo", 1) -> List(new TopicPartition("testTopic", 0), new TopicPartition("testTopic", 1)),
    HostPort("bar", 1) -> List(new TopicPartition("testTopic", 2), new TopicPartition("testTopic", 3)))
  private val state2 = Map[HostPort, List[TopicPartition]](
    HostPort("foo", 1) -> List(new TopicPartition("testTopic", 0)),
    HostPort("bar", 1) -> List(new TopicPartition("testTopic", 2), new TopicPartition("testTopic", 3)),
    HostPort("baz", 1) -> List(new TopicPartition("testTopic", 1)))

  private def createTrackingActor: ActorRef = {
    val trackingActor = system.actorOf(KafkaConsumerStateTrackingActor.props)
    val probe = TestProbe()
    probe.send(trackingActor, StateUpdated(initialState))
    probe.expectMsg(Ack)
    trackingActor
  }
  "KafkaConsumerStateTrackingActor" should {
    "Immediately return partition assignments for newly registered actors" in {
      val trackingActor = createTrackingActor
      val probe = TestProbe()

      trackingActor ! Register(probe.ref)
      probe.expectMsg(PartitionAssignments(initialState))
    }

    "Return partition assignments if asked" in {
      val trackingActor = createTrackingActor
      val probe = TestProbe()

      probe.send(trackingActor, GetPartitionAssignments)
      probe.expectMsg(PartitionAssignments(initialState))
    }

    "Update any registered actors when a new cluster state is received" in {
      val trackingActor = createTrackingActor
      val registeredProbe = TestProbe()
      val updatingProbe = TestProbe()

      trackingActor ! Register(registeredProbe.ref)
      registeredProbe.expectMsg(PartitionAssignments(initialState))

      updatingProbe.send(trackingActor, StateUpdated(state2))
      updatingProbe.expectMsg(Ack)
      registeredProbe.expectMsg(PartitionAssignments(state2))
    }
  }
}

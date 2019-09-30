// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, Address }
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory }
import com.ultimatesoftware.kafka.KafkaConsumerStateTrackingActor.Register
import com.ultimatesoftware.kafka.{ KafkaPartitionShardRouterActor, PartitionRegion }
import com.ultimatesoftware.kafka.streams.{ AggregateStateStoreKafkaStreams, GlobalKTableMetadataHandler, KafkaPartitionMetadata, KafkaStreamsKeyValueStore }
import com.ultimatesoftware.scala.core.kafka.{ HostPort, PartitionAssignments }
import com.ultimatesoftware.scala.core.monitoring.metrics.NoOpMetricsProvider
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito._
import org.scalatest.{ Assertion, BeforeAndAfterAll, Matchers, WordSpecLike }
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Future
import scala.concurrent.duration._

class GenericAggregateActorRouterSpec extends TestKit(ActorSystem("GenericAggregateActorRouterSpec")) with WordSpecLike with Matchers
  with BeforeAndAfterAll with MockitoSugar with TestBoundedContext {

  import KafkaPartitionShardRouterActor._

  private implicit val timeout: Timeout = Timeout(10.seconds)
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val config: Config = ConfigFactory.load()
  private val localHostname = config.getString("akka.remote.netty.tcp.hostname")

  private def routerActor(partitionTrackerProbe: TestProbe): ActorRef = {
    val mockGlobalKTableStateStore = mock[GlobalKTableMetadataHandler]
    val mockKafkaStreamsCommand = mock[AggregateStateStoreKafkaStreams]
    val mockStateMetaRepo = mock[KafkaStreamsKeyValueStore[String, KafkaPartitionMetadata]]

    when(mockStateMetaRepo.all()).thenReturn(Future.successful(List.empty))
    when(mockStateMetaRepo.allValues()).thenReturn(Future.successful(List.empty))

    when(mockGlobalKTableStateStore.stateMetaQueryableStore).thenReturn(mockStateMetaRepo)

    val shardRegionCreator = new TPRegionProps(kafkaStreamsLogic, NoOpMetricsProvider, mockGlobalKTableStateStore,
      mockKafkaStreamsCommand)

    val props = KafkaPartitionShardRouterActor.props(
      partitionTracker = partitionTrackerProbe.ref,
      partitioner = kafkaStreamsLogic.partitioner,
      trackedTopic = kafkaStreamsLogic.stateTopic,
      regionCreator = shardRegionCreator, extractEntityId = GenericAggregateActor.CommandEnvelope.extractEntityId)

    system.actorOf(props)
  }

  def getPartitionRegions(assignments: Map[HostPort, List[TopicPartition]]): Map[Int, Address] = {
    assignments.flatMap {
      case (hostPort, topicPartition) ⇒
        val actorAddress = if (hostPort.host == localHostname) {
          Address("akka", system.name)
        } else {
          Address("akka.tcp", system.name, hostPort.host, hostPort.port)
        }
        topicPartition.map { tp ⇒
          tp.partition() -> actorAddress
        }
    }
  }

  private def expectAddresses(testProbe: TestProbe, router: ActorRef, expectedRegionAddresses: Map[Int, Address]): Assertion = {
    testProbe.send(router, GetPartitionRegionAssignments)
    val actualRegionAddressed = testProbe.receiveOne(5.seconds).asInstanceOf[Map[Int, PartitionRegion]]
      .mapValues(_.regionManager.anchorPath.address)
    actualRegionAddressed shouldEqual expectedRegionAddresses
  }

  "RouterActor" should {
    val hostPort1 = HostPort("foo", 2552)
    val hostPort2 = HostPort("bar", 2552)
    val hostPort3 = HostPort("baz", 2552)
    val hostPortLocal = HostPort(localHostname, 2552)
    val partition1 = new TopicPartition(kafkaStreamsLogic.stateTopic.name, 1)
    val partition2 = new TopicPartition(kafkaStreamsLogic.stateTopic.name, 2)
    val partition3 = new TopicPartition(kafkaStreamsLogic.stateTopic.name, 3)
    val partition4 = new TopicPartition(kafkaStreamsLogic.stateTopic.name, 4)
    val partition5 = new TopicPartition(kafkaStreamsLogic.stateTopic.name, 5)

    "Correctly follow partition region assignment updates" in {
      val partitionTrackerProbe = TestProbe()
      val testProbe = TestProbe()

      val initialPartitionAssignments: Map[HostPort, List[TopicPartition]] = Map(
        hostPort1 -> List(partition1),
        hostPort2 -> List(partition2, partition4),
        hostPortLocal -> List(partition3, partition5))

      val router = routerActor(partitionTrackerProbe)
      partitionTrackerProbe.expectMsg(Register(router))
      partitionTrackerProbe.reply(PartitionAssignments(initialPartitionAssignments))

      val expectedRegionAddresses = getPartitionRegions(initialPartitionAssignments)
      expectAddresses(testProbe, router, expectedRegionAddresses)

      // Reassign some partitions
      val newPartitionAssignments: Map[HostPort, List[TopicPartition]] = Map(
        hostPort1 -> List(partition1, partition4),
        hostPortLocal -> List(partition3),
        hostPort3 -> List(partition2, partition5))
      partitionTrackerProbe.send(router, PartitionAssignments(newPartitionAssignments))

      val expectedRegionAddresses2 = getPartitionRegions(newPartitionAssignments)
      expectAddresses(testProbe, router, expectedRegionAddresses2)
    }
  }

}

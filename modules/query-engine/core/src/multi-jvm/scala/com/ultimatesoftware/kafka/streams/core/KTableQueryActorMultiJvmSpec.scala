// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor.Props
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.testkit.{ImplicitSender, TestProbe}
import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.kafka.streams.KafkaStreamsKeyValueStore
import com.ultimatesoftware.kafka.streams.core.KTableQueryActor.GetState
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{HostInfo, StreamsMetadata}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

trait STMultiNodeSpec extends MultiNodeSpecCallbacks with AnyWordSpecLike with Matchers with BeforeAndAfterAll {
  self: MultiNodeSpec =>

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  // Might not be needed anymore if we find a nice way to tag all logging from a node
  override implicit def convertToWordSpecStringWrapper(s: String): WordSpecStringWrapper =
    new WordSpecStringWrapper(s"$s (on node '${self.myself.name}', $getClass)")
}

object KTableQueryActorSpecConfig extends MultiNodeConfig {
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
  val nodesConfig = ConfigFactory.parseString("""
    akka.actor.allow-java-serialization=on
    akka.actor.warn-about-java-serializer-usage=off
    """)
  commonConfig(nodesConfig)
}

class KTableQueryActorMultiJvmNode1 extends KTableQueryActorSpecBase
class KTableQueryActorMultiJvmNode2 extends KTableQueryActorSpecBase


class KTableQueryActorSpecBase extends MultiNodeSpec(KTableQueryActorSpecConfig) with STMultiNodeSpec
  with ImplicitSender with MockitoSugar {
  import KTableQueryActorSpecConfig._

  override def initialParticipants: Int = roles.size

  private val node1Agg = "agg1"
  private val node2Agg = "agg2"
  private val missingAgg = "missing"

  private val node1Address = node(node1).address
  private val node2Address = node(node2).address

  private val node1HostInfo = new HostInfo(node1Address.host.get, node1Address.port.get)
  private val node2HostInfo = new HostInfo(node2Address.host.get, node2Address.port.get)

  private def setupTestContext(): TestContext = {
    import ArgumentMatchers._
    val mockStreams = mock[KafkaStreams]
    when(mockStreams.metadataForKey(anyString, ArgumentMatchers.eq(node1Agg), any[StringSerializer]))
      .thenReturn(new StreamsMetadata(node1HostInfo, Set.empty[String].asJava, Set.empty[TopicPartition].asJava))
    when(mockStreams.metadataForKey(anyString, ArgumentMatchers.eq(node2Agg), any[StringSerializer]))
      .thenReturn(new StreamsMetadata(node2HostInfo, Set.empty[String].asJava, Set.empty[TopicPartition].asJava))
    when(mockStreams.metadataForKey(anyString, ArgumentMatchers.eq(missingAgg), any[StringSerializer]))
      .thenReturn(new StreamsMetadata(node1HostInfo, Set.empty[String].asJava, Set.empty[TopicPartition].asJava))

    val keyValueStoreMap = Map(node1Agg -> node1Agg, node2Agg -> node2Agg)
    val mockKeyValueStore = mock[KafkaStreamsKeyValueStore[String, String]]
    when(mockKeyValueStore.get(anyString)).thenAnswer((invocation: InvocationOnMock) ⇒ {
      val id = invocation.getArgument[String](0)
      Future.successful(keyValueStoreMap.get(id))
    })

    val queryActorProps = KTableQueryActor.props(mockStreams, "TestStore", mockKeyValueStore)
    TestContext(queryActorProps)
  }
  case class TestContext(queryActorProps: Props)

  "KTableQueryActor" should {
    "Fetch state for locally assigned partitions" in {
      val testContext = setupTestContext()
      import testContext._
      val probe = TestProbe()
      val queryActor = system.actorOf(queryActorProps, "KTableQueryActorLocalPartitionTest")

      runOn(node1) {
        probe.send(queryActor, GetState(node1Agg))
        probe.expectMsg(KTableQueryActor.FetchedState(node1Agg, Some(node1Agg)))
        probe.send(queryActor, GetState(missingAgg))
        probe.expectMsg(KTableQueryActor.FetchedState(missingAgg, None))
      }
      enterBarrier("localAssignmentTestEnd")
    }

    "Fetch state from remote peers for partitions assigned to other nodes" in {
      val testContext = setupTestContext()
      import testContext._
      val probe = TestProbe()
      val queryActor = system.actorOf(queryActorProps, "KTableQueryActorRemotePartitionTest")

      runOn(node1) {
        probe.send(queryActor, GetState(node2Agg))
        probe.expectMsg(10.seconds, KTableQueryActor.FetchedState(node2Agg, Some(node2Agg)))
      }
      enterBarrier("remoteAssignmentTestEnd")
    }
  }
}

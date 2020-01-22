// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka

import akka.actor.{ Actor, DeadLetter, Props }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks }
import akka.testkit.{ ImplicitSender, TestProbe }
import com.ultimatesoftware.scala.core.kafka.{ HostPort, KafkaProducerTrait, KafkaTopic, PartitionAssignments }
import org.apache.kafka.common.TopicPartition
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import org.scalatestplus.mockito.MockitoSugar

trait STMultiNodeSpec extends MultiNodeSpecCallbacks with WordSpecLike with Matchers with BeforeAndAfterAll {
  self: MultiNodeSpec =>

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  // Might not be needed anymore if we find a nice way to tag all logging from a node
  override implicit def convertToWordSpecStringWrapper(s: String): WordSpecStringWrapper =
    new WordSpecStringWrapper(s"$s (on node '${self.myself.name}', $getClass)")
}

object KafkaPartitionShardRouterActorSpecConfig extends MultiNodeConfig {
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
}

class KafkaPartitionShardRouterActorMultiJvmNode1 extends KafkaPartitionShardRouterActorSpecBase
class KafkaPartitionShardRouterActorMultiJvmNode2 extends KafkaPartitionShardRouterActorSpecBase

object KafkaPartitionShardRouterActorSpecBase {
  case class Command(id: String)
  case class WrappedCmd(topicPartition: TopicPartition, cmd: Command)

  class ProbeInterceptorActor(topicPartition: TopicPartition, probe: TestProbe) extends Actor {
    override def receive: Receive = {
      case cmd: Command ⇒ probe.ref.forward(WrappedCmd(topicPartition, cmd))
    }
  }

  class ProbeInterceptorRegionCreator(probe: TestProbe) extends TopicPartitionRegionCreator {
    override def propsFromTopicPartition(topicPartition: TopicPartition): Props = Props(new ProbeInterceptorActor(topicPartition, probe))
  }
}
class KafkaPartitionShardRouterActorSpecBase extends MultiNodeSpec(KafkaPartitionShardRouterActorSpecConfig) with STMultiNodeSpec
  with ImplicitSender with MockitoSugar {
  import KafkaPartitionShardRouterActorSpecBase._
  import KafkaPartitionShardRouterActorSpecConfig._

  override def initialParticipants: Int = roles.size

  private val trackedTopic = KafkaTopic("test")

  private val partitionMappings = Map(
    "partition0" -> 0,
    "partition1" -> 1,
    "partition2" -> 2,
    "partition1Again" -> 1)
  private val partition0 = new TopicPartition(trackedTopic.name, 0)
  private val partition1 = new TopicPartition(trackedTopic.name, 1)
  private val partition2 = new TopicPartition(trackedTopic.name, 2)
  private val node1Address = node(node1).address
  private val node2Address = node(node2).address

  private val partitionAssignments = Map[HostPort, List[TopicPartition]](
    HostPort(node1Address.host.get, node1Address.port.get) -> List(partition0, partition1),
    HostPort(node2Address.host.get, node2Address.port.get) -> List(partition2))

  case class TestContext(partitionProbe: TestProbe, regionProbe: TestProbe, shardRouterProps: Props)

  case object ThrowExceptionInExtractEntityId
  private def setupTestContext(): TestContext = {
    val partitionProbe = TestProbe()
    val regionProbe = TestProbe()

    val producer = mock[KafkaProducerTrait[String, Array[Byte]]]
    when(producer.topic).thenReturn(trackedTopic)
    when(producer.partitionFor(anyString)).thenAnswer((invocation: InvocationOnMock) ⇒ {
      val key = invocation.getArgument[String](0)
      partitionMappings.get(key)
    })

    val extractEntityId: PartialFunction[Any, String] = {
      case cmd: Command ⇒ cmd.id
      case ThrowExceptionInExtractEntityId => throw new RuntimeException("Received ThrowExceptionInExtractEntityId in extractEntityId function")
    }
    val shardRouterProps = Props(new KafkaPartitionShardRouterActor(
      partitionTracker = partitionProbe.ref,
      kafkaStateProducer = producer,
      regionCreator = new ProbeInterceptorRegionCreator(regionProbe),
      extractEntityId = extractEntityId))

    TestContext(partitionProbe = partitionProbe, regionProbe = regionProbe, shardRouterProps = shardRouterProps)
  }

  private def initializePartitionAssignments(partitionProbe: TestProbe): Unit = {
    partitionProbe.expectMsgType[KafkaConsumerStateTrackingActor.Register]
    partitionProbe.reply(PartitionAssignments(partitionAssignments))
  }

  "KafkaPartitionShardRouterActor" should {
    "Create a new region and forward messages to locally assigned partitions" in {
      val testContext = setupTestContext()
      import testContext._
      val probe = TestProbe()
      val routerActor = system.actorOf(shardRouterProps, "RouterActorLocalPartitionTest")

      initializePartitionAssignments(partitionProbe)

      runOn(node1) {
        val command0 = Command("partition0")
        probe.send(routerActor, command0)
        regionProbe.expectMsg(WrappedCmd(partition0, command0))
        regionProbe.reply(command0)
        probe.expectMsg(command0)
      }
    }

    "Forward messages that belong to a remote partition" in {
      val testContext = setupTestContext()
      import testContext._
      val probe = TestProbe()
      val routerActor = system.actorOf(shardRouterProps, "RouterActorRemotePartitionTest")

      initializePartitionAssignments(partitionProbe)

      val command0 = Command("partition0")
      runOn(node2) {
        probe.send(routerActor, command0)
      }
      runOn(node1) {
        regionProbe.expectMsg(WrappedCmd(partition0, command0))
        regionProbe.reply(command0)
      }
      runOn(node2) {
        probe.expectMsg(command0)
      }
    }

    "Handle updates to partition assignments" in {
      val testContext = setupTestContext()
      val probe = TestProbe()
      import testContext._

      val routerActor = system.actorOf(shardRouterProps, "RouterActorUpdatedPartitionsTest")

      initializePartitionAssignments(partitionProbe)

      val newPartitionAssignments = Map[HostPort, List[TopicPartition]](
        HostPort(node1Address.host.get, node1Address.port.get) -> List(partition0, partition1, partition2),
        HostPort(node2Address.host.get, node2Address.port.get) -> List())

      partitionProbe.send(routerActor, PartitionAssignments(newPartitionAssignments))

      runOn(node1) {
        val command = Command("partition2")
        probe.send(routerActor, command)
        regionProbe.expectMsg(WrappedCmd(partition2, command))
        regionProbe.reply(command)
        probe.expectMsg(command)
      }
    }

    "Stash messages before initialized" in {
      runOn(node1) {
        val testContext = setupTestContext()
        val probe = TestProbe()
        import testContext._
        val routerActor = system.actorOf(shardRouterProps)

        val command0 = Command("partition0")
        probe.send(routerActor, command0)

        initializePartitionAssignments(partitionProbe)

        regionProbe.expectMsg(WrappedCmd(partition0, command0))
        regionProbe.reply(command0)
        probe.expectMsg(command0)
      }
    }

    "Send messages that can't be routed to dead letters" in {
      val testContext = setupTestContext()
      import testContext._

      val deadLetterProbe = TestProbe()
      system.eventStream.subscribe(deadLetterProbe.ref, classOf[DeadLetter])
      val routerActor = system.actorOf(shardRouterProps)

      initializePartitionAssignments(partitionProbe)

      routerActor ! ThrowExceptionInExtractEntityId

      val dead = deadLetterProbe.expectMsgType[DeadLetter]
      dead.message shouldEqual ThrowExceptionInExtractEntityId
      dead.sender shouldEqual routerActor
      dead.recipient shouldEqual system.deadLetters
    }
  }
}

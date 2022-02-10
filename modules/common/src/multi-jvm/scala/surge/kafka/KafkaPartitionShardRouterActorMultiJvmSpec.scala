// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka

import akka.actor.DeadLetter
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.testkit.{ImplicitSender, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.internal.tracing.TracedMessage
import surge.internal.utils.DiagnosticContextFuturePropagation

import scala.util.Random

trait STMultiNodeSpec extends MultiNodeSpecCallbacks with AnyWordSpecLike with Matchers with BeforeAndAfterAll {
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
  val nodesConfig: Config = ConfigFactory.parseString("""
    akka.actor.allow-java-serialization=off
    akka.actor.warn-about-java-serializer-usage=on
    """)
  commonConfig(nodesConfig)
}

class KafkaPartitionShardRouterActorMultiJvmNode1 extends KafkaPartitionShardRouterActorSpecBase
class KafkaPartitionShardRouterActorMultiJvmNode2 extends KafkaPartitionShardRouterActorSpecBase

class KafkaPartitionShardRouterActorSpecBase
    extends MultiNodeSpec(KafkaPartitionShardRouterActorSpecConfig)
    with STMultiNodeSpec
    with ImplicitSender
    with KafkaPartitionShardRouterActorSpecLike {
  import KafkaPartitionShardRouterActorSpecModels._
  import KafkaPartitionShardRouterActorSpecConfig._

  val ec = DiagnosticContextFuturePropagation.global

  override def initialParticipants: Int = roles.size

  private val node1Address = node(node1).address
  private val node2Address = node(node2).address

  val partitionAssignments: Map[HostPort, List[TopicPartition]] = Map[HostPort, List[TopicPartition]](
    HostPort(node1Address.host.get, node1Address.port.get) -> List(partition0, partition1),
    HostPort(node2Address.host.get, node2Address.port.get) -> List(partition2))

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
  }
}

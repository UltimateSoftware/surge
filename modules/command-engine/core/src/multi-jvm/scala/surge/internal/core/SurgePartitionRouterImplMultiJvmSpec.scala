// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.core

import akka.actor.ActorSystem
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks }
import akka.testkit.{ ImplicitSender, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import io.github.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import surge.core.{ Ack, TestBoundedContext }
import surge.health.SignalType
import surge.health.config.{ ThrottleConfig, WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ Error, HealthSignal }
import surge.health.matchers.{ SideEffectBuilder, SignalPatternMatcherDefinition }
import surge.internal.akka.kafka.{ KafkaConsumerPartitionAssignmentTracker, KafkaConsumerStateTrackingActor }
import surge.internal.health.StreamMonitoringRef
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import surge.internal.utils.DiagnosticContextFuturePropagation
import surge.kafka.streams.{ AggregateStateStoreKafkaStreams, MockPartitionTracker }
import surge.kafka.{ HostPort, KafkaProducerTrait, KafkaTopic, PartitionAssignments }
import surge.metrics.Metrics

import java.util.regex.Pattern
import scala.concurrent.duration._

trait STMultiNodeSpec extends MultiNodeSpecCallbacks with AnyWordSpecLike with Matchers with BeforeAndAfterAll {
  self: MultiNodeSpec =>

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  // Might not be needed anymore if we find a nice way to tag all logging from a node
  override implicit def convertToWordSpecStringWrapper(s: String): WordSpecStringWrapper =
    new WordSpecStringWrapper(s"$s (on node '${self.myself.name}', $getClass)")
}

object SurgePartitionRouterImplSpecConfig extends MultiNodeConfig {
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
  val nodesConfig: Config = ConfigFactory.parseString(s"""
    akka.actor.allow-java-serialization=off
    akka.actor.warn-about-java-serializer-usage=on
    """)
  commonConfig(nodesConfig)
}

class SurgePartitionRouterImplMultiJvmNode1 extends SurgePartitionRouterImplSpecBase
class SurgePartitionRouterImplMultiJvmNode2 extends SurgePartitionRouterImplSpecBase

trait SurgePartitionRouterImplSpecLike extends TestBoundedContext with MockitoSugar {
  import SurgePartitionRouterImplSpecModels._

  case class TestContext(partitionProbe: TestProbe, regionProbe: TestProbe, partitionRouter: SurgePartitionRouterImpl)

  implicit def actorSystem: ActorSystem
  def partitionAssignments: Map[HostPort, List[TopicPartition]]

  private val defaultConfig = ConfigFactory.load()

  implicit val ec = DiagnosticContextFuturePropagation.global

  private val trackedTopic = KafkaTopic("test")
  val partition0 = new TopicPartition(trackedTopic.name, 0)
  val partition1 = new TopicPartition(trackedTopic.name, 1)
  val partition2 = new TopicPartition(trackedTopic.name, 2)
  private val partitionMappings = Map("partition0" -> 0, "partition1" -> 1, "partition2" -> 2, "partition1Again" -> 1)

  def createTestContext(): TestContext = {
    val signalStreamProvider = createSignalStream()

    val kafkaStreamsImpl: AggregateStateStoreKafkaStreams = new AggregateStateStoreKafkaStreams(
      businessLogic.aggregateName,
      businessLogic.kafka.stateTopic,
      (streams: KafkaStreams) => new MockPartitionTracker(streams),
      applicationHostPort = Some("localhost:1234"),
      applicationId = "test-app",
      clientId = businessLogic.kafka.clientId,
      signalStreamProvider.bus(),
      actorSystem,
      Metrics.globalMetricRegistry,
      defaultConfig)

    val partitionProbe = TestProbe()
    val partitionTracker = new KafkaConsumerPartitionAssignmentTracker(partitionProbe.ref)

    val regionProbe = TestProbe()
    val cqrsRegionCreator = new ProbeInterceptorRegionCreator(regionProbe)

    val producer = mock[KafkaProducerTrait[String, Array[Byte]]]
    when(producer.topic).thenReturn(trackedTopic)
    when(producer.partitionFor(anyString)).thenAnswer((invocation: InvocationOnMock) => {
      val key = invocation.getArgument[String](0)
      partitionMappings.get(key)
    })

    val actorRouter: SurgePartitionRouterImpl =
      new SurgePartitionRouterImpl(
        defaultConfig,
        actorSystem,
        partitionTracker,
        businessLogic,
        kafkaStreamsImpl,
        cqrsRegionCreator,
        signalStreamProvider.bus(),
        isAkkaClusterEnabled = false,
        Some(producer))

    TestContext(partitionProbe, regionProbe, actorRouter)
  }

  def initializePartitionAssignments(partitionProbe: TestProbe, assignments: Map[HostPort, List[TopicPartition]] = partitionAssignments): Unit = {
    partitionProbe.expectMsgType[KafkaConsumerStateTrackingActor.Register]
    partitionProbe.reply(PartitionAssignments(assignments))
  }

  private def createSignalStream() = {
    val probe = TestProbe()
    new SlidingHealthSignalStreamProvider(
      WindowingStreamConfig(
        advancerConfig = WindowingStreamSliderConfig(buffer = 10, advanceAmount = 1),
        throttleConfig = ThrottleConfig(elements = 100, duration = 5.seconds),
        windowingInitDelay = 10.milliseconds,
        windowingResumeDelay = 10.milliseconds,
        maxWindowSize = 500),
      actorSystem,
      Some(new StreamMonitoringRef(probe.ref)),
      patternMatchers = Seq(
        SignalPatternMatcherDefinition
          .repeating(times = 1, pattern = Pattern.compile("baz"), frequency = 100.milliseconds)
          .withSideEffect(
            SideEffectBuilder()
              .addSideEffectSignal(
                HealthSignal(topic = "health.signal", name = "it.failed", data = Error("it.failed", None), signalType = SignalType.ERROR, source = None))
              .buildSideEffect())))
  }
}
class SurgePartitionRouterImplSpecBase
    extends MultiNodeSpec(SurgePartitionRouterImplSpecConfig)
    with STMultiNodeSpec
    with ScalaFutures
    with ImplicitSender
    with EmbeddedKafka
    with Eventually
    with SurgePartitionRouterImplSpecLike {
  import SurgePartitionRouterImplSpecConfig._
  import TestBoundedContext._

  override def initialParticipants: Int = roles.size

  override val actorSystem: ActorSystem = system

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(15, Seconds)), interval = scaled(Span(50, Milliseconds)))

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 6001)

  private val node1Address = node(node1).address
  private val node2Address = node(node2).address

  val partitionAssignments: Map[HostPort, List[TopicPartition]] = Map[HostPort, List[TopicPartition]](
    HostPort(node1Address.host.get, node1Address.port.get) -> List(partition0, partition1),
    HostPort(node2Address.host.get, node2Address.port.get) -> List(partition2))

  "KafkaPartitionShardRouterActor" should {
    val testContext = createTestContext()
    import testContext._
    "Start router actor successfully" in {
      partitionRouter.controllable.start().futureValue shouldBe Ack
      initializePartitionAssignments(partitionProbe)
    }

    "Create a new region and forward messages to locally assigned partitions" in {
      val probe = TestProbe()
      val routerActor = partitionRouter.actorRegion

      runOn(node1) {
        val command0 = Increment("partition0")
        probe.send(routerActor, command0)
        regionProbe.expectMsg(WrappedTestCommand(partition0, command0))
        regionProbe.reply(command0)
        probe.expectMsg(command0)
      }
    }

    "Forward messages that belong to a remote partition" in {
      val probe = TestProbe()
      val routerActor = partitionRouter.actorRegion
      val command0 = Increment("partition0")
      runOn(node2) {
        probe.send(routerActor, command0)
      }
      runOn(node1) {
        regionProbe.expectMsg(WrappedTestCommand(partition0, command0))
        regionProbe.reply(command0)
      }
      runOn(node2) {
        probe.expectMsg(command0)
      }
    }
  }
}

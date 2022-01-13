// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import io.github.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.streams.KafkaStreams
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{ BeforeAndAfterAll, PrivateMethodTester }
import surge.core.TestBoundedContext.{ BaseTestCommand, BaseTestEvent, State }
import surge.core.{ Ack, TestBoundedContext }
import surge.health.config.{ ThrottleConfig, WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ Error, HealthSignal }
import surge.health.matchers.{ SideEffectBuilder, SignalPatternMatcherDefinition }
import surge.health.{ ComponentRestarted, HealthListener, HealthMessage, SignalType }
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.core.SurgePartitionRouterImpl
import surge.internal.health.StreamMonitoringRef
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import surge.kafka.streams.{ AggregateStateStoreKafkaStreams, MockPartitionTracker }
import surge.metrics.Metrics

import java.util.regex.Pattern
import scala.concurrent.duration._

trait SurgeMessagePipelineSpecLike extends TestBoundedContext {

  implicit def actorSystem: ActorSystem

  def defaultConfig: Config

  case class TestContext(
      probe: TestProbe,
      signalStreamProvider: SlidingHealthSignalStreamProvider,
      pipeline: SurgeMessagePipeline[State, BaseTestCommand, BaseTestEvent])
  def createTestContext(): TestContext = {
    val probe = TestProbe()

    // Create a SignalStreamProvider
    val signalStreamProvider = new SlidingHealthSignalStreamProvider(
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

    // Create SurgeMessagePipeline
    lazy val pipeline = createPipeline(signalStreamProvider)

    TestContext(probe, signalStreamProvider, pipeline)
  }

  private def createPipeline(signalStreamProvider: SlidingHealthSignalStreamProvider): SurgeMessagePipeline[State, BaseTestCommand, BaseTestEvent] = {
    new SurgeMessagePipeline[State, BaseTestCommand, BaseTestEvent](actorSystem, businessLogic, signalStreamProvider, defaultConfig) {

      override def actorSystem: ActorSystem = system

      private val isAkkaClusterEnabled = defaultConfig.getBoolean("surge.feature-flags.experimental.enable-akka-cluster")
      override protected lazy val actorRouter: SurgePartitionRouterImpl =
        new SurgePartitionRouterImpl(
          defaultConfig,
          actorSystem,
          new KafkaConsumerPartitionAssignmentTracker(stateChangeActor),
          businessLogic,
          kafkaStreamsImpl,
          cqrsRegionCreator,
          signalStreamProvider.bus(),
          isAkkaClusterEnabled,
          None)
      override protected val kafkaStreamsImpl: AggregateStateStoreKafkaStreams = new AggregateStateStoreKafkaStreams(
        businessLogic.aggregateName,
        businessLogic.kafka.stateTopic,
        (streams: KafkaStreams) => new MockPartitionTracker(streams),
        applicationHostPort = Some("localhost:1234"),
        applicationId = "test-app",
        clientId = businessLogic.kafka.clientId,
        signalStreamProvider.bus(),
        system,
        Metrics.globalMetricRegistry,
        defaultConfig)

      override def shutdownSignalPatterns(): Seq[Pattern] = Seq(Pattern.compile("kafka.streams.fatal.retries.exceeded.error"))
    }
  }

}
class SurgeMessagePipelineSpec
    extends TestKit(ActorSystem("SurgeMessagePipelineSpec", ConfigFactory.load("artery-test-config")))
    with AnyWordSpecLike
    with ScalaFutures
    with EmbeddedKafka
    with Eventually
    with PrivateMethodTester
    with TestBoundedContext
    with BeforeAndAfterAll
    with Matchers
    with SurgeMessagePipelineSpecLike {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(15, Seconds)), interval = scaled(Span(50, Milliseconds)))

  override val defaultConfig: Config = ConfigFactory.load()
  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = defaultConfig.getInt("kafka.port"))

  override val actorSystem: ActorSystem = system

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
    createCustomTopic(
      topic = businessLogic.kafka.stateTopic.name,
      partitions = 5,
      topicConfig = Map(TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT))
    createCustomTopic(topic = businessLogic.kafka.eventsTopic.name, partitions = 5)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    pipeline.controllable.stop().futureValue shouldBe an[Ack]
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
    EmbeddedKafka.stop()
    super.afterAll()
  }

  val ctx: TestContext = createTestContext()
  val pipeline: SurgeMessagePipeline[State, BaseTestCommand, BaseTestEvent] = ctx.pipeline
  val signalStreamProvider: SlidingHealthSignalStreamProvider = ctx.signalStreamProvider
  val probe: TestProbe = ctx.probe

  "SurgeMessagePipeline" should {
    "start successfully" in {
      pipeline.controllable.start().futureValue shouldBe an[Ack]
    }

    "subscribe to health.signals via stream on start" in {
      // Stream should be a subscriber
      signalStreamProvider
        .bus()
        .subscriberInfo()
        .exists(s => s.name == "surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamImpl") shouldEqual true

    }

    "inject signal named `it.failed` into signal stream" in {

      var captured: Option[HealthSignal] = None
      pipeline.signalBus.subscribe(
        subscriber = new HealthListener() {
          override def id(): String = "pipelineTestSignalListener"

          override def handleMessage(message: HealthMessage): Unit = {
            message match {
              case signal: HealthSignal =>
                if (signal.name == "it.failed") {
                  captured = Some(signal)
                }
              case _ =>
            }
          }
        },
        to = pipeline.signalBus.signalTopic())

      pipeline.signalBus.signalWithError(name = "baz", Error("baz happened", None)).emit()

      eventually {
        captured.nonEmpty shouldEqual true
      }
    }

    "Register all wrapped components with the HealthSignalBus" in {
      val bus = pipeline.signalBus
      // Retrieve Registrations and verify SurgeMessagePipeline is registered
      eventually {
        val registrations = bus.registrations().futureValue
        val registrationNames = registrations.map(_.componentName)
        registrationNames should contain(s"surge-message-pipeline-${businessLogic.aggregateName}")

        registrationNames should contain(s"state-store-kafka-streams-${businessLogic.aggregateName}")
        val kStreamsRegistration = registrations.find(r => r.componentName == s"state-store-kafka-streams-${businessLogic.aggregateName}").get
        kStreamsRegistration.restartSignalPatterns.map(p => p.pattern()) should contain("kafka.streams.fatal.error")
      }
    }

    "Restart registered components" in {
      // Kafka Streams Restart
      pipeline.signalBus.signalWithError(name = "kafka.streams.fatal.error", Error("boom", None)).emit()
      eventually {
        val restarted = probe.fishForMessage(max = 2.seconds) { case msg: Any =>
          msg.isInstanceOf[ComponentRestarted]
        }

        Option(restarted).nonEmpty shouldEqual true
        restarted.asInstanceOf[ComponentRestarted].componentName shouldEqual s"state-store-kafka-streams-${businessLogic.aggregateName}"
      }
    }

    "stop successfully" in {
      val stopped = pipeline.controllable.stop()

      val result = stopped.futureValue

      result shouldEqual Ack
    }

    "restart successfully" in {
      val restarted = pipeline.controllable.restart()

      val result = restarted.futureValue
      result shouldEqual Ack
    }

    "shutdown when kafka streams fails to start too many times" in {
      // Get running signal bus so we can check registrations
      val bus = pipeline.signalBus

      val componentName = s"surge-message-pipeline-${businessLogic.aggregateName}"
      eventually {
        whenReady(bus.registrations(matching = Pattern.compile(componentName))) { registrations =>
          registrations.size shouldEqual 1

          bus.signalWithError(name = "kafka.streams.fatal.retries.exceeded.error", Error("fake shutdown trigger", None)).emit()

          // Wait for the surge-message-pipeline to be unregistered on termination.
          eventually {
            whenReady(bus.registrations()) { registrations =>
              registrations.exists(r => r.componentName == componentName) shouldEqual false
            }
          }
        }
      }
    }

    "unregister all child components after stopping" in {
      pipeline.controllable.start().futureValue shouldEqual Ack
      pipeline.controllable.stop().futureValue shouldEqual Ack

      eventually {
        pipeline.signalBus.registrations().futureValue shouldBe empty
      }
    }
  }

}

// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.streams.KafkaStreams
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{ BeforeAndAfterAll, Ignore, PrivateMethodTester }
import play.api.libs.json.JsValue
import surge.core.{ Ack, TestBoundedContext }
import surge.health.config.{ ThrottleConfig, WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.domain.{ Error, HealthSignal }
import surge.health.matchers.{ SideEffectBuilder, SignalPatternMatcherDefinition }
import surge.health.supervisor.Api.ShutdownComponent
import surge.health.{ ComponentRestarted, HealthListener, HealthMessage, SignalType }
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.core.SurgePartitionRouterImpl
import surge.internal.health.StreamMonitoringRef
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import surge.kafka.streams.{ AggregateStateStoreKafkaStreams, MockPartitionTracker }
import surge.metrics.Metrics

import java.util.regex.Pattern
import scala.concurrent.duration._

// FIXME need to be able to stop the router actor for this to work
@Ignore
class SurgeMessagePipelineSpec
    extends TestKit(ActorSystem("SurgeMessagePipelineSpec", ConfigFactory.load("artery-test-config")))
    with AnyWordSpecLike
    with ScalaFutures
    with EmbeddedKafka
    with Eventually
    with PrivateMethodTester
    with TestBoundedContext
    with BeforeAndAfterAll
    with Matchers {
  import TestBoundedContext._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Seconds)), interval = scaled(Span(10, Milliseconds)))

  private val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 6001)
  private val defaultConfig = ConfigFactory.load()

  case class TestContext(
      probe: TestProbe,
      signalStreamProvider: SlidingHealthSignalStreamProvider,
      pipeline: SurgeMessagePipeline[State, BaseTestCommand, BaseTestEvent])

  def withTestContext[T](testFun: TestContext => T): T = {
    val probe = TestProbe()

    // Create a SignalStreamProvider
    val signalStreamProvider = new SlidingHealthSignalStreamProvider(
      WindowingStreamConfig(
        advancerConfig = WindowingStreamSliderConfig(buffer = 10, advanceAmount = 1),
        throttleConfig = ThrottleConfig(elements = 100, duration = 5.seconds),
        windowingInitDelay = 10.milliseconds,
        windowingResumeDelay = 10.milliseconds,
        maxWindowSize = 500),
      system,
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
    val pipeline = createPipeline(signalStreamProvider)
    // Start Pipeline
    pipeline.controllable.start().futureValue shouldBe an[Ack]

    try {
      testFun(TestContext(probe, signalStreamProvider, pipeline))
    } finally {
      pipeline.controllable.stop().futureValue shouldBe an[Ack]
    }
  }

  override def afterAll(): Unit = {
    // FIXME verifySystemShutdown should be true, but this does not shut down in a reasonable amount of time
    TestKit.shutdownActorSystem(system, duration = 30.seconds, verifySystemShutdown = false)
  }

  "SurgeMessagePipeline" should {
    "stop successfully" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        withTestContext { ctx =>
          import ctx._
          createCustomTopic(businessLogic.kafka.eventsTopic.name, Map.empty)
          createCustomTopic(businessLogic.kafka.stateTopic.name, Map.empty)

          val stopped = pipeline.controllable.stop()

          val result = stopped.futureValue

          result shouldEqual Ack()
        }
      }
    }

    "restart successfully" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        withTestContext { ctx =>
          import ctx._
          createCustomTopic(businessLogic.kafka.eventsTopic.name, Map.empty)
          createCustomTopic(businessLogic.kafka.stateTopic.name, Map.empty)

          val restarted = pipeline.controllable.restart()

          val result = restarted.futureValue
          result shouldEqual Ack()
        }
      }
    }

    "subscribe to health.signals via stream on start" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        withTestContext { ctx =>
          import ctx._
          createCustomTopic(businessLogic.kafka.eventsTopic.name, Map.empty)
          createCustomTopic(businessLogic.kafka.stateTopic.name, Map.empty)

          // Stream should be a subscriber
          signalStreamProvider
            .bus()
            .subscriberInfo()
            .exists(s => s.name == "surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamImpl") shouldEqual true
        }
      }
    }

    "Register all wrapped components with the HealthSignalBus" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        withTestContext { ctx =>
          import ctx._

          createCustomTopic(businessLogic.kafka.eventsTopic.name, Map.empty)
          createCustomTopic(businessLogic.kafka.stateTopic.name, Map.empty)

          val bus = pipeline.signalBus
          // Retrieve Registrations and verify SurgeMessagePipeline is registered
          eventually {
            val registrations = bus.registrations().futureValue
            val registrationNames = registrations.map(_.componentName)
            registrationNames should contain("surge-message-pipeline")

            registrationNames should contain("state-store-kafka-streams")
            val kStreamsRegistration = registrations.find(r => r.componentName == "state-store-kafka-streams").get
            kStreamsRegistration.restartSignalPatterns.map(p => p.pattern()) should contain("kafka.streams.fatal.error")

            registrationNames should contain("router-actor")
            val routerActorRegistration = registrations.find(r => r.componentName == "router-actor").get
            routerActorRegistration.restartSignalPatterns.map(p => p.pattern()) should contain("kafka.fatal.error")
          }
        }
      }
    }

    "shutdown when kafka streams fails to start too many times" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        withTestContext { ctx =>
          import ctx._

          createCustomTopic(businessLogic.kafka.eventsTopic.name, Map.empty)
          createCustomTopic(businessLogic.kafka.stateTopic.name, Map.empty)

          // Get running signal bus so we can check registrations
          val bus = pipeline.signalBus

          eventually {
            whenReady(bus.registrations(matching = Pattern.compile("surge-message-pipeline"))) { registrations =>
              registrations.size shouldEqual 1

              bus.signalWithError(name = "kafka.streams.fatal.retries.exceeded.error", Error("fake shutdown trigger", None)).emit()

              // Wait for the surge-message-pipeline to be unregistered on termination.
              eventually {
                whenReady(bus.registrations()) { registrations =>
                  registrations.exists(r => r.componentName == "surge-message-pipeline") shouldEqual false
                }
              }
            }
          }
        }
      }
    }

    "have router-actor unregistered when terminated" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        withTestContext { ctx =>
          import ctx._

          createCustomTopic(businessLogic.kafka.eventsTopic.name, Map.empty)
          createCustomTopic(businessLogic.kafka.stateTopic.name, Map.empty)

          // wait for router-actor to be registered
          val registration = eventually {
            val reg = pipeline.signalBus.registrations().futureValue.find(_.componentName == "router-actor")
            reg shouldBe defined
            reg
          }
          // Shutdown the router-actor
          registration.get.controlProxyRef ! ShutdownComponent("router-actor", probe.ref)

          // Wait for the router-actor to be unregistered on termination.
          eventually {
            pipeline.signalBus.registrations().futureValue.map(_.componentName) should not contain "router-actor"
          }
        }
      }
    }

    "Restart registered components" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        withTestContext { ctx =>
          import ctx._

          createCustomTopic(businessLogic.kafka.eventsTopic.name, Map.empty)
          createCustomTopic(businessLogic.kafka.stateTopic.name, Map.empty)

          // Kafka Streams Restart
          pipeline.signalBus.signalWithError(name = "kafka.streams.fatal.error", Error("boom", None)).emit()
          eventually {
            val restarted = probe.fishForMessage(max = 2.seconds) { case msg: Any =>
              msg.isInstanceOf[ComponentRestarted]
            }

            Option(restarted).nonEmpty shouldEqual true
            restarted.asInstanceOf[ComponentRestarted].componentName shouldEqual "state-store-kafka-streams"
          }

          // Router Actor Restart
          pipeline.signalBus.signalWithError(name = "kafka.fatal.error", Error("boom", None)).emit()
          eventually {
            val restarted = probe.fishForMessage(max = FiniteDuration(3, "seconds")) { case msg: Any =>
              msg.isInstanceOf[ComponentRestarted]
            }

            Option(restarted).nonEmpty shouldEqual true
            restarted.asInstanceOf[ComponentRestarted].componentName shouldEqual "router-actor"
          }
        }
      }
    }

    "unregister all child components after stopping" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        withTestContext { ctx =>
          import ctx._

          createCustomTopic(businessLogic.kafka.eventsTopic.name, Map.empty)
          createCustomTopic(businessLogic.kafka.stateTopic.name, Map.empty)

          // wait for router-actor to be registered
          eventually {
            val reg = pipeline.signalBus.registrations().futureValue
            reg.exists(p => p.componentName == "router-actor") shouldEqual true
          }

          val acknowledgedStop: Ack = pipeline.controllable.stop().futureValue
          acknowledgedStop shouldEqual Ack()

          val afterStopRegistrations = eventually {
            val reg = pipeline.signalBus.registrations().futureValue
            reg.isEmpty shouldEqual true
            reg
          }

          afterStopRegistrations.isEmpty shouldEqual true
        }
      }
    }

    "inject signal named `it.failed` into signal stream" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        withTestContext { ctx =>
          import ctx._

          createCustomTopic(businessLogic.kafka.eventsTopic.name, Map.empty)
          createCustomTopic(businessLogic.kafka.stateTopic.name, Map.empty)

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
      }
    }
  }

  private def createPipeline(signalStreamProvider: SlidingHealthSignalStreamProvider): SurgeMessagePipeline[State, BaseTestCommand, BaseTestEvent] = {
    new SurgeMessagePipeline[State, BaseTestCommand, BaseTestEvent](system, businessLogic, signalStreamProvider, defaultConfig) {

      override def actorSystem: ActorSystem = system

      private val isAkkaClusterEnabled = config.getBoolean("surge.feature-flags.experimental.enable-akka-cluster")
      override protected val kafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue] = new AggregateStateStoreKafkaStreams[JsValue](
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

      override protected val actorRouter: SurgePartitionRouterImpl =
        new SurgePartitionRouterImpl(
          defaultConfig,
          actorSystem,
          new KafkaConsumerPartitionAssignmentTracker(stateChangeActor),
          businessLogic,
          kafkaStreamsImpl,
          cqrsRegionCreator,
          signalStreamProvider.bus(),
          isAkkaClusterEnabled)

      override def shutdownSignalPatterns(): Seq[Pattern] = Seq(Pattern.compile("kafka.streams.fatal.retries.exceeded.error"))
    }
  }
}

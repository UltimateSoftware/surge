// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.internal.domain

import java.util.regex.Pattern

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.streams.KafkaStreams
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import play.api.libs.json.{JsValue, Json}
import surge.core.TestBoundedContext
import surge.health.config.{WindowingStreamConfig, WindowingStreamSliderConfig}
import surge.health.domain.Error
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.core.SurgePartitionRouterImpl
import surge.internal.health.StreamMonitoringRef
import surge.internal.health.supervisor.RestartComponentAttempted
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider
import surge.kafka.streams.{AggregateStateStoreKafkaStreams, MockPartitionTracker, MockState}
import surge.metrics.Metrics

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.languageFeature.postfixOps

class SurgeMessagePipelineSpec
    extends TestKit(ActorSystem("SurgeMessagePipelineSpec", ConfigFactory.load("artery-test-config")))
    with AnyWordSpecLike
    with EmbeddedKafka
    with Eventually
    with TestBoundedContext
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers {
  import TestBoundedContext._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(160, Seconds)), interval = scaled(Span(5, Seconds)))


  private val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 6001)

  private var probe: TestProbe = _
  private var signalStreamProvider: SlidingHealthSignalStreamProvider = _
  private var pipeline: SurgeMessagePipeline[State, BaseTestCommand, Nothing, BaseTestEvent] = _

  override def beforeEach(): Unit = {
    createCustomTopic(kafkaConfig.eventsTopic.name, Map.empty)
    createCustomTopic(kafkaConfig.stateTopic.name, Map.empty)

    val config = ConfigFactory.load()
    probe = TestProbe()

    // Create a SignalStreamProvider
    signalStreamProvider = new SlidingHealthSignalStreamProvider(
      WindowingStreamConfig(advancerConfig = WindowingStreamSliderConfig()),
      system,
      Some(new StreamMonitoringRef(probe.ref)))
    // Create SurgeMessagePipeline
    pipeline = pipeline(signalStreamProvider, config)
    // Start Pipeline
    pipeline.start()
  }

  override def afterEach(): Unit = {
    // Stop Pipeline
    Option(pipeline).foreach(cmd => cmd.stop())
  }

  override def afterAll(): Unit = {
    system.terminate()
  }

  "SurgeMessagePipeline" should {
    "subscribe to health.signals via stream on start" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        // Stream should be a subscriber
        signalStreamProvider
          .busWithSupervision()
          .subscriberInfo()
          .exists(s => s.name == "surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamImpl") shouldEqual true
      }
    }

    "have self registered with HealthSignalBus" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        val bus = pipeline.signalBus
        // Retrieve Registrations and verify SurgeMessagePipeline is registered
        eventually {
          val registrations = Await.result(bus.registrations(), 10.seconds)
          val registration = registrations.find(r => r.name == "surge-message-pipeline")

          registration.nonEmpty shouldEqual true
        }
      }
    }

    "have state-store-kafka-streams registered with HealthSignalBus" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        // Get running signal bus so we can check registrations
        val bus = pipeline.signalBus

        // Retrieve Registrations and verify AggregateStateStore is registered
        //  Verify the restartSignalPatterns are as expected
        eventually {
          val registrations = Await.result(bus.registrations(), 10.seconds)
          val registration = registrations.find(r => r.name == "state-store-kafka-streams")

          registration.nonEmpty shouldEqual true

          registration.get.restartSignalPatterns.map(p => p.pattern()).contains("kafka.streams.fatal.error") shouldEqual true
        }
      }
    }

    "shutdown when kafka streams fails to start too many times" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        // Get running signal bus so we can check registrations
        val bus = pipeline.signalBus

        eventually {
          val registrations = Await.result(bus.registrations(), 10.seconds)
          val registration = registrations.find(r => r.name == "surge-message-pipeline")

          registration.nonEmpty shouldEqual true

          bus.signalWithError(name = "kafka.streams.fatal.retries.exceeded.error", Error("fake shutdown trigger", None)).emit()

          // Wait for the surge-message-pipeline to be unregistered on termination.
          eventually {
            val registrations = Await.result(pipeline.signalBus.registrations(), atMost = FiniteDuration(20, "seconds"))
            registrations.exists(r => r.name == "surge-message-pipeline") shouldEqual false
          }
        }
      }
    }

    "have router-actor registered with HealthSignalBus" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        // Get running signal bus so we can check registrations
        val bus = pipeline.signalBus

        eventually {
          val registrations = Await.result(bus.registrations(), 10.seconds)
          val registration = registrations.find(r => r.name == "router-actor")

          registration.nonEmpty shouldEqual true

          registration.get.restartSignalPatterns.map(p => p.pattern()).contains("kafka.fatal.error") shouldEqual true
        }
      }
    }

    "have router-actor unregistered when terminated" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        // wait for router-actor to be registered
        eventually {
          val registrations = Await.result(pipeline.signalBus.registrations(), FiniteDuration(10, "seconds"))
          val registration = registrations.find(r => r.name == "router-actor")

          registration.nonEmpty shouldEqual true
          // Poison the router-actor
          registration.get.ref ! PoisonPill

          // Wait for the router-actor to be unregistered on termination.
          eventually {
            val registrations = Await.result(pipeline.signalBus.registrations(), FiniteDuration(10, "seconds"))
            registrations.exists(r => r.name == "router-actor") shouldEqual false
          }

        }
      }
    }

    "attempt to restart state-store-kafka-streams" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        pipeline.signalBus.signalWithError(name = "kafka.streams.fatal.error", Error("boom", None)).emit()

        eventually {
          val restartAttempt = probe.fishForMessage(max = FiniteDuration(1, "seconds")) { case _: RestartComponentAttempted =>
            true
          }

          Option(restartAttempt).nonEmpty shouldEqual true
          restartAttempt.asInstanceOf[RestartComponentAttempted].componentName shouldEqual "state-store-kafka-streams"
        }
      }
    }

    "attempt to restart router-actor" in {
      withRunningKafkaOnFoundPort(config) { _ =>
        pipeline.signalBus.signalWithError(name = "kafka.fatal.error", Error("boom", None)).emit()

        eventually {
          val restartAttempt = probe.fishForMessage(max = FiniteDuration(1, "seconds")) { case _: RestartComponentAttempted =>
            true
          }

          Option(restartAttempt).nonEmpty shouldEqual true
          restartAttempt.asInstanceOf[RestartComponentAttempted].componentName shouldEqual "router-actor"
        }
      }
    }
  }

  private def mockValidator(key: String, newValue: Array[Byte], oldValue: Option[Array[Byte]]): Boolean = {
    val newValueObj = Json.parse(newValue).as[MockState]
    newValueObj.string == "state" + newValueObj.int
  }

  private def pipeline(
      signalStreamProvider: SlidingHealthSignalStreamProvider,
      config: Config): SurgeMessagePipeline[State, BaseTestCommand, Nothing, BaseTestEvent] = {
    new SurgeMessagePipeline[State, BaseTestCommand, Nothing, BaseTestEvent](system, businessLogic, signalStreamProvider, config) {
      override def actorSystem: ActorSystem = system

      override protected val actorRouter: SurgePartitionRouterImpl =
        new SurgePartitionRouterImpl(
          actorSystem,
          new KafkaConsumerPartitionAssignmentTracker(stateChangeActor),
          businessLogic,
          cqrsRegionCreator,
          signalStreamProvider.busWithSupervision())
      override protected val kafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue] = new AggregateStateStoreKafkaStreams[JsValue](
        businessLogic.aggregateName,
        kafkaConfig.stateTopic,
        (streams: KafkaStreams) => new MockPartitionTracker(streams),
        aggregateValidator = mockValidator,
        applicationHostPort = Some("localhost:1234"),
        applicationId = "test-app",
        clientId = kafkaConfig.clientId,
        signalStreamProvider.busWithSupervision(),
        system,
        Metrics.globalMetricRegistry)

      override def shutdownSignalPatterns(): Seq[Pattern] = Seq(Pattern.compile("kafka.streams.fatal.retries.exceeded.error"))
    }

  }
}

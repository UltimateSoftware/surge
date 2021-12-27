// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.testkit.TestKit
import com.google.protobuf.ByteString
import com.typesafe.config.ConfigFactory
import com.ukg.surge.multilanguage.EmbeddedKafkaSpecSupport.Available
import com.ukg.surge.multilanguage.TestBoundedContext._
import com.ukg.surge.multilanguage.protobuf.HealthCheckReply.Status
import com.ukg.surge.multilanguage.protobuf.{ Command, ForwardCommandReply, ForwardCommandRequest, GetStateRequest, HealthCheckReply, HealthCheckRequest }
import io.github.embeddedkafka.EmbeddedKafka._
import io.github.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.config.TopicConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AsyncWordSpecLike
import play.api.libs.json.Json
import surge.core.Ack
import surge.scaladsl.command.SurgeCommand

import java.util.UUID

class MultilanguageGatewayServiceImplSpec
    extends TestKit(ActorSystem("MultilanguageGatewayServiceImplSpec"))
    with AsyncWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with TestBoundedContext
    with EmbeddedKafkaSpecSupport {

  import system.dispatcher

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds), interval = Span(50, Milliseconds))

  private val defaultConfig = ConfigFactory.load()
  private val logger: LoggingAdapter = Logging(system, classOf[MultilanguageGatewayServiceImplSpec])
  private val kafkaPort = defaultConfig.getInt("kafka.port")
  private val zookeeperPort = defaultConfig.getInt("zookeeper.port")
  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = kafkaPort, zooKeeperPort = zookeeperPort)

  val aggregateName: String = defaultConfig.getString("surge-server.aggregate-name")
  val eventsTopicName: String = defaultConfig.getString("surge-server.events-topic")
  val stateTopicName: String = defaultConfig.getString("surge-server.state-topic")
  val testBusinessLogicService = new TestBusinessLogicService()
  val genericSurgeCommandBusinessLogic = new GenericSurgeCommandBusinessLogic(aggregateName, eventsTopicName, stateTopicName, testBusinessLogicService)

  val testSurgeEngine: SurgeCommand[UUID, SurgeState, SurgeCmd, SurgeEvent] = {
    val engine = SurgeCommand(system, genericSurgeCommandBusinessLogic, system.settings.config)
    engine.start()
    logger.info("Started engine!")
    engine
  }

  val multilanguageGatewayService = new MultilanguageGatewayServiceImpl(testSurgeEngine)

  override def beforeAll(): Unit = {
    super.beforeAll()
    val _ = EmbeddedKafka.start()
    expectedServerStatus(kafkaPort, Available)
    expectedServerStatus(zookeeperPort, Available)

    createCustomTopic(eventsTopicName, partitions = 3)
    createCustomTopic(stateTopicName, partitions = 3, topicConfig = Map(TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT))
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    testSurgeEngine.stop().futureValue shouldBe an[Ack]
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "MultilanguageGatewayServiceImpl" should {
    val aggregateId = UUID.randomUUID().toString
    val initialState = AggregateState(aggregateId, 1, 1)
    val lastState = AggregateState(aggregateId, 1, 3)
    "create new aggregate and return a state" in {
      val cmd: BaseTestCommand = Increment(aggregateId)
      val serializedCmd = Json.toJson(cmd).toString().getBytes()
      val pbCmd = Command(aggregateId, payload = ByteString.copyFrom(serializedCmd))
      val request = ForwardCommandRequest(aggregateId, Some(pbCmd))

      val response = multilanguageGatewayService.forwardCommand(request).map {
        case reply: ForwardCommandReply if reply.isSuccess =>
          reply.newState match {
            case Some(pbState) =>
              Json.parse(pbState.payload.toByteArray).asOpt[AggregateState]
            case None =>
              Option.empty[AggregateState]
          }
        case reply =>
          reply.newState
      }

      response.futureValue shouldEqual Some(initialState)
    }

    "handle increment command and fetch updated state for an existing aggregate" in {
      val cmd: BaseTestCommand = Increment(aggregateId)
      val serializedCmd = Json.toJson(cmd).toString().getBytes()
      val pbCmd = Command(aggregateId, payload = ByteString.copyFrom(serializedCmd))
      val request = ForwardCommandRequest(aggregateId, Some(pbCmd))

      val response = multilanguageGatewayService.forwardCommand(request).map {
        case reply: ForwardCommandReply if reply.isSuccess =>
          reply.newState match {
            case Some(pbState) =>
              Json.parse(pbState.payload.toByteArray).asOpt[AggregateState]
            case None =>
              Option.empty[AggregateState]
          }
        case reply =>
          reply.newState
      }

      val expectedSecondState = AggregateState(aggregateId, 2, 2)
      response.futureValue shouldEqual Some(expectedSecondState)
    }

    "handle decrement command and fetch updated state for an existing aggregate" in {
      val cmd: BaseTestCommand = Decrement(aggregateId)
      val serializedCmd = Json.toJson(cmd).toString().getBytes()
      val pbCmd = Command(aggregateId, payload = ByteString.copyFrom(serializedCmd))
      val request = ForwardCommandRequest(aggregateId, Some(pbCmd))

      val response = multilanguageGatewayService.forwardCommand(request).map {
        case reply: ForwardCommandReply if reply.isSuccess =>
          reply.newState match {
            case Some(pbState) =>
              Json.parse(pbState.payload.toByteArray).asOpt[AggregateState]
            case None =>
              Option.empty[AggregateState]
          }
        case reply =>
          reply.newState
      }

      response.futureValue shouldEqual Some(lastState)
    }

    "fail to process the incorrect command" in {
      val cmd: BaseTestCommand = ExceptionThrowingCommand(aggregateId)
      val serializedCmd = Json.toJson(cmd).toString().getBytes()
      val pbCmd = Command(aggregateId, payload = ByteString.copyFrom(serializedCmd))
      val request = ForwardCommandRequest(aggregateId, Some(pbCmd))

      val response = multilanguageGatewayService.forwardCommand(request).map(_.isSuccess)

      response.futureValue shouldEqual false
    }

    "fetch the state of existing aggregate" in {
      val request = GetStateRequest(aggregateId)

      val response = multilanguageGatewayService.getState(request).map { reply =>
        reply.state match {
          case Some(pbState) =>
            Json.parse(pbState.payload.toByteArray).asOpt[AggregateState]
          case None =>
            Option.empty[AggregateState]
        }
      }

      response.futureValue shouldEqual Some(lastState)
    }

    "fetch the status of surge engine" in {
      val request = HealthCheckRequest()

      val response = multilanguageGatewayService.healthCheck(request)

      response.futureValue shouldEqual HealthCheckReply(status = Status.UP)
    }
  }

}

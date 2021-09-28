// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.testkit.TestKit
import com.google.protobuf.ByteString
import com.typesafe.config.ConfigFactory
import com.ukg.surge.multilanguage.TestBoundedContext._
import com.ukg.surge.multilanguage.protobuf.{ Command, ForwardCommandReply, ForwardCommandRequest, GetStateRequest }
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.config.TopicConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AsyncWordSpecLike
import play.api.libs.json.Json

import java.util.UUID

class MultilanguageGatewayServiceImplSpec
    extends TestKit(ActorSystem("MultilanguageGatewayServiceImplSpec"))
    with AsyncWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with EmbeddedKafka
    with TestBoundedContext {

  import system.dispatcher

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
    createCustomTopic(eventsTopicName, partitions = 2)
    createCustomTopic(stateTopicName, partitions = 2, topicConfig = Map(TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT))
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds), interval = Span(50, Milliseconds))

  private val config = ConfigFactory.load()
  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = config.getInt("kafka.port"))
  private val logger: LoggingAdapter = Logging(system, classOf[MultilanguageGatewayServiceImplSpec])

  val aggregateName: String = config.getString("surge-server.aggregate-name")
  val eventsTopicName: String = config.getString("surge-server.events-topic")
  val stateTopicName: String = config.getString("surge-server.state-topic")
  val testBusinessLogicService = new TestBusinessLogicService()
  val multilanguageGatewayService = new MultilanguageGatewayServiceImpl(testBusinessLogicService, aggregateName, eventsTopicName, stateTopicName)

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
  }

}

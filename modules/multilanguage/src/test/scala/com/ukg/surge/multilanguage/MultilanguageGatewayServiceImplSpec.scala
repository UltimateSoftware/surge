// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.google.protobuf.ByteString
import com.typesafe.config.ConfigFactory
import com.ukg.surge.multilanguage.TestBoundedContext.{ AggregateState, Increment }
import com.ukg.surge.multilanguage.protobuf.{
  BusinessLogicService,
  Command,
  ForwardCommandReply,
  ForwardCommandRequest,
  HandleEventsRequest,
  HandleEventsResponse,
  ProcessCommandReply,
  ProcessCommandRequest
}
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.config.TopicConfig
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{ AnyWordSpec, AsyncWordSpecLike }
import org.scalatestplus.mockito.MockitoSugar.mock
import play.api.libs.json.Json

import java.util.UUID
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class MultilanguageGatewayServiceImplSpec
    extends TestKit(ActorSystem("MultilanguageGatewayServiceImplSpec"))
    with AsyncWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with EmbeddedKafka
    with TestBoundedContext {

  import system.dispatcher

  override def afterAll(): Unit = {
    multilanguageGatewayService.surgeEngine.stop()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  private val config = ConfigFactory.load()
  private val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = config.getInt("kafka.port"), zooKeeperPort = config.getInt("zookeeper.port"))

  val aggregateName: String = config.getString("surge-server.aggregate-name")
  val eventsTopicName: String = config.getString("surge-server.events-topic")
  val stateTopicName: String = config.getString("surge-server.state-topic")
  val testBusinessLogicService = new TestBusinessLogicService()
  val multilanguageGatewayService = new MultilanguageGatewayServiceImpl(testBusinessLogicService, aggregateName, eventsTopicName, stateTopicName)
  "MultilanguageGatewayServiceImpl" should {
    "create custom topic" in {
      withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
        createCustomTopic(eventsTopicName, partitions = 5)
        createCustomTopic(stateTopicName, partitions = 5, topicConfig = Map(TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT))

        val aggregateId = UUID.randomUUID().toString
        val cmd = Json.toJson(Increment(aggregateId)).toString().getBytes()
        val pbCmd = Command(aggregateId, payload = ByteString.copyFrom(cmd))
        val request = ForwardCommandRequest(aggregateId, Some(pbCmd))

        val response = multilanguageGatewayService.forwardCommand(request).collect {
          case reply: ForwardCommandReply if reply.isSuccess =>
            reply.newState match {
              case Some(pbState) =>
                Json.parse(pbState.payload.toByteArray).asOpt[AggregateState]
              case None =>
                Option.empty[AggregateState]
            }
        }

        response.futureValue shouldEqual Some(AggregateState(aggregateId, 1, 1))
      }
    }
  }

}

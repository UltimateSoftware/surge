// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.testkit.TestKit
import com.google.protobuf.ByteString
import com.typesafe.config.ConfigFactory
import com.ukg.surge.multilanguage.TestBoundedContext.{AggregateState, Increment}
import com.ukg.surge.multilanguage.protobuf.{BusinessLogicService, Command, ForwardCommandReply, ForwardCommandRequest, HandleEventsRequest, HandleEventsResponse, ProcessCommandReply, ProcessCommandRequest}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.config.TopicConfig
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpecLike}
import org.scalatestplus.mockito.MockitoSugar.mock
import play.api.libs.json.Json

import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

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
  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = config.getInt("kafka.port"), zooKeeperPort = config.getInt("zookeeper.port"))
  private val logger: LoggingAdapter = Logging(system, classOf[MultilanguageGatewayServiceImplSpec])

  val aggregateName: String = config.getString("surge-server.aggregate-name")
  val eventsTopicName: String = config.getString("surge-server.events-topic")
  val stateTopicName: String = config.getString("surge-server.state-topic")
  val testBusinessLogicService = new TestBusinessLogicService()
  val multilanguageGatewayService = new MultilanguageGatewayServiceImpl(testBusinessLogicService, aggregateName, eventsTopicName, stateTopicName)

  "MultilanguageGatewayServiceImpl" should {
    val aggregateId = UUID.randomUUID().toString
    "return new state" in {
        val cmd = Json.toJson(Increment(aggregateId)).toString().getBytes()
        val pbCmd = Command(aggregateId, payload = ByteString.copyFrom(cmd))
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
          logger.error(reply.rejectionMessage)
          reply.newState
      }

        response.futureValue shouldEqual Some(AggregateState(aggregateId, 1, 1))
    }

  }

}

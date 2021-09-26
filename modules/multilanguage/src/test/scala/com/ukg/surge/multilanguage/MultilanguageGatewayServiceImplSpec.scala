package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import com.ukg.surge.multilanguage.protobuf.{BusinessLogicService, ForwardCommandReply, ForwardCommandRequest, HandleEventsRequest, HandleEventsResponse, ProcessCommandReply, ProcessCommandRequest}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpecLike}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class MultilanguageGatewayServiceImplSpec
  extends TestKit(ActorSystem("MultilanguageGatewayServiceImplSpec"))
    with AsyncWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  import system.dispatcher
  private val defaultConfig = ConfigFactory.load()

}

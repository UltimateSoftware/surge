// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import akka.stream.Materializer
import com.google.protobuf.ByteString
import com.ukg.surge._
import com.ukg.surge.multilanguage.protobuf._
import org.slf4j.LoggerFactory
import surge.core._
import surge.core.command.AggregateCommandModelCoreTrait
import surge.kafka.KafkaTopic
import surge.scaladsl.command.{ AggregateCommandModel, SurgeCommand, SurgeCommandBusinessLogic }
import surge.scaladsl.common.{ CommandFailure, CommandSuccess }

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

// We need a fix in core Surge to get rid of these case classes
// For some reason, Surge doesn't allow us to serialize protobuf
case class SurgeState(payload: Array[Byte]) {}

case class SurgeEvent(aggregateId: String, payload: Array[Byte]) {}

case class SurgeCmd(aggregateId: String, payload: Array[Byte]) {}

object Implicits {

  //
  // Once we get rid of the case classes above, the implicit convs
  // are not going to be needed
  //

  implicit def byteArrayToByteString(byteArray: Array[Byte]): ByteString = {
    ByteString.copyFrom(byteArray)
  }

  implicit def byteStringToByteArray(byteString: ByteString): Array[Byte] = {
    byteString.toByteArray
  }

  implicit def surgeStateToPbState(state: SurgeState): protobuf.State = {
    protobuf.State(payload = state.payload)
  }

  implicit def surgeEventToPbEvent(event: SurgeEvent): protobuf.Event = {
    protobuf.Event(aggregateId = event.aggregateId, payload = event.payload)
  }

  implicit def surgeCommandToPbCommand(command: SurgeCmd): protobuf.Command = {
    protobuf.Command(aggregateId = command.aggregateId, payload = command.payload)
  }

  implicit def pbEventToSurgeEvent(event: protobuf.Event): SurgeEvent = {
    SurgeEvent(event.aggregateId, payload = event.payload.toByteArray)
  }

  implicit def pbStateToSurgeState(state: protobuf.State): SurgeState = {
    SurgeState(state.payload.toByteArray)
  }

  implicit def pbCommandToSurgeCmd(command: protobuf.Command): SurgeCmd = {
    SurgeCmd(command.aggregateId, payload = command.payload)
  }

}

class MultilanguageGatewayServiceImpl()(implicit sys: ActorSystem, mat: Materializer) extends MultilanguageGatewayService {

  val logger = LoggerFactory.getLogger(classOf[MultilanguageGatewayServiceImpl])

  implicit val ec = sys.dispatcher

  val clientConfig = sys.settings.config.getConfig("business-logic-server")
  val port = clientConfig.getInt("port")
  val host = clientConfig.getString("host")

  val clientSettings = GrpcClientSettings.connectToServiceAt(host, port).withTls(false)

  val bridgeToBusinessApp: BusinessLogicService = BusinessLogicServiceClient(clientSettings)

  val genericCommandModel = new AggregateCommandModel[SurgeState, SurgeCmd, SurgeEvent] {

    import Implicits._
    override def processCommand(aggregate: Option[SurgeState], surgeCommand: SurgeCmd): Try[Seq[SurgeEvent]] = {
      println("Processing command")
      println("Aggregate.isDefined:" + aggregate.isDefined)
      val maybePbState: Option[protobuf.State] = aggregate.map(surgeState => surgeState: protobuf.State)
      val pbCommand: protobuf.Command = surgeCommand: multilanguage.protobuf.Command
      val processCommandRequest = ProcessCommandRequest(maybePbState, Some(pbCommand))
      println("ProcessCommandRequest: " + processCommandRequest)
      try {
        val call = bridgeToBusinessApp.processCommand(processCommandRequest)
        println("Called business app via gRPC!")
        val reply = Await.result(call, atMost = 7.seconds)
        if (reply.rejection == "") {
          Success(reply.events.map(pbEvent => pbEvent: SurgeEvent))
        } else {
          Failure(new Exception(reply.rejection))
        }
      } catch {
        case e: Exception =>
          println("Error making gRPC call to business app from processCommand")
          e.printStackTrace()
          throw e
      }
    }

    override def handleEvent(aggregate: Option[SurgeState], surgeEvent: SurgeEvent): Option[SurgeState] = {
      println("Handling event")
      val maybePbState: Option[protobuf.State] = aggregate.map(surgeState => surgeState: protobuf.State)
      val pbEvent = surgeEvent
      val handleEventRequest = HandleEventRequest(maybePbState, Some(pbEvent))
      val call = bridgeToBusinessApp.handleEvent(handleEventRequest)
      println("Called business app via gRPC!")
      val reply: HandleEventResponse = Await.result(call, atMost = 7.seconds)
      reply.state.map(pbState => pbState: SurgeState)
    }
  }

  val genericSurgeModel = new SurgeCommandBusinessLogic[UUID, SurgeState, SurgeCmd, SurgeEvent] {

    override def commandModel: AggregateCommandModelCoreTrait[SurgeState, SurgeCmd, Nothing, SurgeEvent] = genericCommandModel

    override def eventsTopic: KafkaTopic = KafkaTopic("events")

    import Implicits._
    override def aggregateReadFormatting: SurgeAggregateReadFormatting[SurgeState] = new SurgeAggregateReadFormatting[SurgeState] {
      override def readState(bytes: Array[Byte]): Option[SurgeState] = {
        val pbState: protobuf.State = protobuf.State.parseFrom(bytes)
        Some(pbState)
      }
    }

    override def eventWriteFormatting: SurgeEventWriteFormatting[SurgeEvent] = new SurgeEventWriteFormatting[SurgeEvent] {
      override def writeEvent(evt: SurgeEvent): SerializedMessage = {
        val pbEvent: protobuf.Event = evt
        SerializedMessage(key = evt.aggregateId, value = pbEvent.toByteArray, headers = Map.empty)
      }
    }

    override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[SurgeState] = new SurgeAggregateWriteFormatting[SurgeState] {
      override def writeState(surgeState: SurgeState): SerializedAggregate = {
        val pbState: protobuf.State = surgeState
        new SerializedAggregate(pbState.toByteArray, Map.empty)
      }
    }

    override def aggregateName: String = "aggregate" // should come from an environment variable ?
    // what do we do about scenarios where the developer wants to host multiple aggregates in one app unit ?

    override def stateTopic: KafkaTopic = KafkaTopic("state")
  }

  lazy val surgeEngine: SurgeCommand[UUID, SurgeState, SurgeCmd, Nothing, SurgeEvent] = {
    val engine = SurgeCommand(sys, genericSurgeModel, sys.settings.config)
    engine.start()
    engine
  }

  import Implicits._
  override def sendCommand(in: SendCommandRequest): Future[SendCommandReply] = {
    in.command match {
      case Some(cmd: protobuf.Command) =>
        println("Received:" + cmd)
        val aggIdStr = cmd.aggregateId
        val aggIdUUID: UUID = UUID.fromString(aggIdStr)
        val surgeCmd: SurgeCmd = cmd
        println("Forwarding:" + surgeCmd)
        surgeEngine.aggregateFor(aggIdUUID).sendCommand(surgeCmd).map {
          case CommandSuccess(aggregateState) =>
            SendCommandReply(successMessage = "Success!") // TODO: decide what else to include here
          case CommandFailure(reason) =>
            reason.printStackTrace()
            SendCommandReply(rejectionMessage = reason.getMessage)
        }

      case None =>
        // this should not happen
        // log warning message
        Future.successful(SendCommandReply())
    }
  }
}

class MultilanguageGatewayServer(system: ActorSystem) {
  def run(): Future[Http.ServerBinding] = {
    // Akka boot up code
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    val config = system.settings.config.getConfig("surge-server")
    val host = config.getString("host")
    val port = config.getInt("port")

    // Create service handlers
    val service: HttpRequest => Future[HttpResponse] =
      MultilanguageGatewayServiceHandler(new MultilanguageGatewayServiceImpl())

    val binding = Http().newServerAt(host, port).bind(service)

    // report successful binding
    binding.foreach { binding => println(s"gRPC server bound to: ${binding.localAddress}") }

    binding

  }
}

object MultilanguageSidecarMain extends App {
  implicit val system = ActorSystem("multilanguage")
  import system.dispatcher
  val binding = new MultilanguageGatewayServer(system).run()
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      binding.map(_.terminate(hardDeadline = 7.seconds))
    }
  })
}

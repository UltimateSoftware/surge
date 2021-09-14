// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import com.typesafe.config.Config
import com.ukg.surge.multilanguage.protobuf._
import io.micrometer.influx.{InfluxApiVersion, InfluxConfig, InfluxConsistency}

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class MultilanguageGatewayServer(system: ActorSystem) {


  val config: InfluxConfig = new InfluxConfig() {
    override def org = "ukg"

    override def bucket = "ukg-bucket"

    override def token = "iHExG2IEiaw1RmHEf5TXdyixh6M0DLYxn0-sIljH6ouR6qkeNAzjQ5n3G7rM4Sxnws0LkkvpJ6XL5_AH5odv-g=="
    // FIXME: This should be securely bound rather than hard-coded, of course.

    override def get(k: String): String = k // accept the rest of the defaults

    override def step: Duration = Duration.ofSeconds(10)

    override def db = "surge-metric"

    override def connectTimeout(): Duration = Duration.ofSeconds(1800)

    override def readTimeout(): Duration = Duration.ofSeconds(1800)

    override def batchSize(): Int = 1

    override def apiVersion(): InfluxApiVersion = InfluxApiVersion.V1

    override def consistency(): InfluxConsistency = InfluxConsistency.ALL

    override def uri(): String = "http://localhost:8086/"

    override def numThreads(): Int = 10

    override def autoCreateDb(): Boolean = true

  }


  def run(): Future[Http.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    val logger: LoggingAdapter = Logging(system, classOf[MultilanguageGatewayServer])

    val config = system.settings.config.getConfig("surge-server")
    val host = config.getString("host")
    val port = config.getInt("port")

    val aggregateName: String = config.getString("aggregate-name")
    val eventsTopicName: String = config.getString("events-topic")
    val stateTopicName: String = config.getString("state-topic")

    logger.info(s"""Binding multilanguage gateway server on $host:$port.
         |Aggregate name: $aggregateName.
         |Events topic: $eventsTopicName.
         |State topic: ${stateTopicName}""".stripMargin)

    val service: HttpRequest => Future[HttpResponse] =
      MultilanguageGatewayServiceHandler(new MultilanguageGatewayServiceImpl(aggregateName, eventsTopicName, stateTopicName))

    val binding = Http().newServerAt(host, port).bind(service)

    // report successful binding
    binding.foreach { binding => logger.info(s"gRPC server bound to: ${binding.localAddress}") }

    binding

  }
}

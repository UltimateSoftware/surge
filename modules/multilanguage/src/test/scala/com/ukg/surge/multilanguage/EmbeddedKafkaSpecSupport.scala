package com.ukg.surge.multilanguage

import com.ukg.surge.multilanguage.EmbeddedKafkaSpecSupport.{ Available, NotAvailable, ServerStatus }

import java.net.{ InetAddress, Socket }
import org.scalatest.Assertion
import org.scalatest.concurrent.{ Eventually, IntegrationPatience }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Milliseconds, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike

import scala.util.{ Failure, Success, Try }

trait EmbeddedKafkaSpecSupport extends AnyWordSpecLike with Matchers with Eventually with IntegrationPatience {

  def expectedServerStatus(port: Int, expectedStatus: ServerStatus): Assertion =
    eventually {
      status(port) shouldBe expectedStatus
    }

  private def status(port: Int): ServerStatus = {
    Try(new Socket(InetAddress.getByName("localhost"), port)) match {
      case Failure(_) => NotAvailable
      case Success(_) => Available
    }
  }
}

object EmbeddedKafkaSpecSupport {
  sealed trait ServerStatus
  case object Available extends ServerStatus
  case object NotAvailable extends ServerStatus
}

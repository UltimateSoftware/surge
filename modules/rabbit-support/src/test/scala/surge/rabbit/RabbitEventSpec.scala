// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package surge.rabbit

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.mockito.Mockito._
import surge.core.SurgeEventWriteFormatting

class RabbitSinkSpec extends TestKit(ActorSystem("RabbitEventSinkSpec")) with AnyWordSpecLike with Matchers {

  "RabbitEventSink" should {
    val sink = new RabbitEventSink[String] {
      override def rabbitMqUri: String = "uri"
      override def queueName: String = "queue"
      override def writeFormatting: SurgeEventWriteFormatting[String] = mock(classOf[SurgeEventWriteFormatting[String]])
    }

    "Have default configs" in {
      sink.autoDeleteQueue shouldEqual false
      sink.durableQueue shouldEqual false
      sink.exclusiveQueue shouldEqual false
      sink.queueArguments shouldEqual Map.empty
      sink.queueName shouldEqual "queue"
      sink.rabbitMqUri shouldEqual "uri"
    }
  }
}
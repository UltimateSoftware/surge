// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import org.scalatest.wordspec.AnyWordSpecLike
import surge.core.{ EventSink, SurgeEventReadFormatting }

import scala.concurrent.Future

// TODO Add some actual tests. Probably need an embedded rabbit somewhere to actually test rabbit source/sink.
//  https://www.testcontainers.org looks interesting for that sort of thing

class RabbitEventSourceSpec extends TestKit(ActorSystem("RabbitEventSourceSpec")) with AnyWordSpecLike {
  class TestRabbitEventSource(val rabbitMqUri: String, val queueName: String) extends RabbitEventSource[String] {
    override def actorSystem: ActorSystem = system
    override def formatting: SurgeEventReadFormatting[String] = (bytes: Array[Byte]) ⇒ new String(bytes)
  }

  class TestProbeSink(probe: TestProbe) extends EventSink[String] {
    override def handleEvent(event: String): Future[Any] = {
      probe.ref ! event
      Future.successful(Done)
    }
  }

  "RabbitEventSource" should {
    "Subscribe to rabbit" ignore {
      val testProbe = TestProbe()
      val rabbitSource = new TestRabbitEventSource("localhost:5762", "temp-test-queue-name")

      rabbitSource.to(new TestProbeSink(testProbe))

      // Publish to rabbit "test message 1"
      testProbe.expectMsg("test message 1")
    }
  }
}

// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.streams.graph

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Source }
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OptionFlowSpec extends TestKit(ActorSystem("OptionFlowSpec")) with AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  "OptionFlow" should {
    "Properly filter None and Some values to the appropriate flow" in {
      val testSink = TestSink.probe[Int]

      val testSource = Source(Vector(Some("hello"), None, Some("wordcount")))
      val optionFlow = OptionFlow(someFlow = Flow[String].map(_.length), noneFlow = Flow[None.type].map(_ => -1))
      val testFlow = testSource.via(optionFlow).runWith(testSink)

      testFlow.request(3).expectNext(5, -1, 9).expectComplete()
    }

    "Fail the stream if either the Some or None flow throws an exception" in {
      val testSink = TestSink.probe[String]
      val expectedException = new RuntimeException("This is expected")

      val someThrowsExceptionFlow = OptionFlow(someFlow = Flow[String].map(_ => throw expectedException), noneFlow = Flow[None.type].map(_ => "not used"))
      val someSource = Source(Vector(Some("hello")))
      val someTestFlow = someSource.via(someThrowsExceptionFlow).runWith(testSink)
      someTestFlow.request(1).expectError(expectedException)

      val noneThrowsExceptionFlow = OptionFlow(someFlow = Flow[String], noneFlow = Flow[None.type].map(_ => throw expectedException))
      val noneSource = Source(Vector(None))
      val noneTestFlow = noneSource.via(noneThrowsExceptionFlow).runWith(testSink)
      noneTestFlow.request(1).expectError(expectedException)

    }
  }
}

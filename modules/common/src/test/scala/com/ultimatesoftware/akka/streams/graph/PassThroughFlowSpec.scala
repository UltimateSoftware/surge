// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.graph

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Keep, Source }
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class PassThroughFlowSpec extends TestKit(ActorSystem("PassThroughFlowSpec")) with AnyWordSpecLike with Matchers {
  private val testSource = Source(Vector("first", "second", "third"))

  "PassThroughFlow" should {
    "Properly pass the original value downstream" in {
      val testSink = TestSink.probe[String]
      val stringLengthFlow = Flow[String].map(_.length)
      val testFlow = testSource.via(PassThroughFlow(stringLengthFlow, Keep.right)).runWith(testSink)

      testFlow.request(3)
        .expectNext("first", "second", "third")
        .expectComplete()
    }

    "Fail the stream if the pass through flow throws an exception" in {
      val testSink = TestSink.probe[String]
      val expectedException = new RuntimeException("This is expected")
      val exceptionalFlow = Flow[String].map(_ ⇒ throw expectedException)
      val testFlow = testSource.via(PassThroughFlow(exceptionalFlow, Keep.right)).runWith(testSink)

      val error = testFlow.request(1)
        .expectError()
      error shouldEqual expectedException
    }
  }
}

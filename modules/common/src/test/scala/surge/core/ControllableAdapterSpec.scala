// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class ControllableAdapterSpec extends AnyWordSpec {

  "Should start" in {
    val adapter = new ControllableAdapter()

    Await.result[Ack](adapter.start(), 10.seconds).leftSide shouldEqual Ack
  }

  "Should stop" in {
    val adapter = new ControllableAdapter()

    Await.result[Ack](adapter.stop(), 10.seconds).leftSide shouldEqual Ack
  }

  "Should shutdown" in {
    val adapter = new ControllableAdapter()

    Await.result[Ack](adapter.shutdown(), 10.seconds).leftSide shouldEqual Ack
  }

  "Should restart" in {
    val adapter = new ControllableAdapter()

    Await.result[Ack](adapter.restart(), 10.seconds).leftSide shouldEqual Ack
  }
}

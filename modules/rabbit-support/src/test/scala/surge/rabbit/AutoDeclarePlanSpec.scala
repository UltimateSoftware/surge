// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class AutoDeclarePlanSpec extends AnyWordSpecLike with Matchers {

  "AutoDeclarePlan" should {
    "Throw IllegalPlanException when queueNames don't match" in {
      an[IllegalPlanException] should be thrownBy AutoDeclarePlan(
        QueuePlan("queue", exclusive = false, durable = false, autoDelete = false),
        ExchangePlan("exchange", durable = false, autoDelete = false),
        Binding("not.queue", "exchange", None)).validate()
    }

    "Throw IllegalPlanException when exchangeNames don't match" in {
      an[IllegalPlanException] should be thrownBy AutoDeclarePlan(
        QueuePlan("queue", exclusive = false, durable = false, autoDelete = false),
        ExchangePlan("exchange", durable = false, autoDelete = false),
        Binding("queue", "not.exchange", Some("route"))).validate()
    }

    "Fail to create declarations when queueNames don't match" in {
      an[IllegalPlanException] should be thrownBy AutoDeclarePlan(
        QueuePlan("queue", exclusive = false, durable = false, autoDelete = false),
        ExchangePlan("exchange", durable = false, autoDelete = false),
        Binding("not.queue", "exchange", None)).declarations()
    }
  }
}

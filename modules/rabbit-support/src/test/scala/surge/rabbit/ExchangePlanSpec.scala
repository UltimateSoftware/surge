// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.stream.alpakka.amqp.{ Declaration, ExchangeDeclaration }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ExchangePlanSpec extends AnyWordSpecLike with Matchers {
  "ExchangePlan" should {
    "Create declaration with durable true" in {
      val declaration: Declaration = ExchangePlan("exchange", durable = true, autoDelete = false).declaration()

      declaration shouldBe a[ExchangeDeclaration]
      declaration.asInstanceOf[ExchangeDeclaration].name shouldEqual "exchange"
      declaration.asInstanceOf[ExchangeDeclaration].durable shouldEqual true
    }

    "Create declaration with autoDelete true" in {
      val declaration: Declaration = ExchangePlan("exchange", durable = false, autoDelete = true).declaration()

      declaration shouldBe a[ExchangeDeclaration]
      declaration.asInstanceOf[ExchangeDeclaration].name shouldEqual "exchange"
      declaration.asInstanceOf[ExchangeDeclaration].autoDelete shouldEqual true
    }

  }
}

// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.stream.alpakka.amqp.{ BindingDeclaration, Declaration }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BindingSpec extends AnyWordSpecLike with Matchers {

  "Binding" should {
    "Create declaration with routingKey" in {
      val declaration: Declaration = Binding("queue", "exchange", Some("route"))
        .declaration()

      declaration shouldBe a[BindingDeclaration]
      declaration.asInstanceOf[BindingDeclaration].routingKey shouldEqual Some("route")
    }

    "Create declaration without routingKey" in {
      val declaration: Declaration = Binding("queue", "exchange", None)
        .declaration()

      declaration shouldBe a[BindingDeclaration]
      declaration.asInstanceOf[BindingDeclaration].routingKey shouldEqual None
    }

    "Create declaration with queue and exchange" in {
      val declaration: Declaration = Binding("queue", "exchange", Some("route"))
        .declaration()

      declaration shouldBe a[BindingDeclaration]
      declaration.asInstanceOf[BindingDeclaration].queue shouldEqual "queue"
      declaration.asInstanceOf[BindingDeclaration].exchange shouldEqual "exchange"
    }
  }
}

// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.stream.alpakka.amqp.{ Declaration, QueueDeclaration }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class QueuePlanSpec extends AnyWordSpecLike with Matchers {
  "QueuePlan" should {
    "Create declaration with durable true" in {
      val declaration: Declaration = QueuePlan("queue", durable = true, exclusive = false, autoDelete = false)
        .declaration()

      declaration shouldBe a[QueueDeclaration]
      declaration.asInstanceOf[QueueDeclaration].name shouldEqual "queue"
      declaration.asInstanceOf[QueueDeclaration].durable shouldEqual true
    }

    "Create declaration with exclusive true" in {
      val declaration: Declaration = QueuePlan("queue", durable = false, exclusive = true, autoDelete = false)
        .declaration()

      declaration shouldBe a[QueueDeclaration]
      declaration.asInstanceOf[QueueDeclaration].name shouldEqual "queue"
      declaration.asInstanceOf[QueueDeclaration].exclusive shouldEqual true
    }

    "Create declaration with autoDelete true" in {
      val declaration: Declaration = QueuePlan("queue", durable = false, exclusive = false, autoDelete = true)
        .declaration()

      declaration shouldBe a[QueueDeclaration]
      declaration.asInstanceOf[QueueDeclaration].name shouldEqual "queue"
      declaration.asInstanceOf[QueueDeclaration].autoDelete shouldEqual true
    }
  }

}

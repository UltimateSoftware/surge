// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.stream.alpakka.amqp.QueueDeclaration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QueueDeclarationExtensionImplicitsSpec extends AnyFlatSpec with Matchers {

  "DeadLetterExchangeKey" should "match" in {
    QueueDeclarationExtensionImplicits.deadLetterExchangeKey shouldEqual "x-dead-letter-exchange"
  }

  "DeadLetterRoutingKeyKey" should "match" in {
    QueueDeclarationExtensionImplicits.deadLetterRoutingKeyKey shouldEqual "x-dead-letter-routing-key"
  }

  "DeadLetterExchange" should "match" in {
    QueueDeclarationExtensionImplicits.QueueDeclarationExtension(QueueDeclaration("queue")).withDeadLetterExchange("foo")
      .arguments(QueueDeclarationExtensionImplicits.deadLetterExchangeKey) shouldEqual "foo"
  }

  "DeadLetterRoutingKey" should "match" in {
    QueueDeclarationExtensionImplicits.QueueDeclarationExtension(QueueDeclaration("queue")).withDeadLetterRoutingKey("foo")
      .arguments(QueueDeclarationExtensionImplicits.deadLetterRoutingKeyKey) shouldEqual "foo"
  }

  "Argument" should "be added" in {
    QueueDeclarationExtensionImplicits.QueueDeclarationExtension(QueueDeclaration("queue")).addArgument("foo", "bar")
      .arguments("foo") shouldEqual "bar"
  }

  "Arguments" should "be added" in {
    QueueDeclarationExtensionImplicits.QueueDeclarationExtension(QueueDeclaration("queue")).addArguments(Map[String, AnyRef]("foo" -> "bar"))
      .arguments("foo") shouldEqual "bar"
  }
}

// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.stream.alpakka.amqp.QueueDeclaration

object QueueDeclarationExtensionImplicits {

  val deadLetterExchangeKey: String = "x-dead-letter-exchange"
  val deadLetterRoutingKeyKey: String = "x-dead-letter-routing-key"

  implicit class QueueDeclarationExtension(queueDeclaration: QueueDeclaration) {
    def addArgument(key: String, value: AnyRef): QueueDeclaration = {
      val originalArguments = queueDeclaration.arguments
      val newArguments = originalArguments + (key -> value)
      queueDeclaration.withArguments(newArguments)
    }

    /**
     * Append all map entries to queueDeclaration.arguments
     * @param arguments
     *   Map[String, AnyRef]
     * @return
     *   QueueDeclaration
     */
    def addArguments(arguments: Map[String, AnyRef]): QueueDeclaration = {
      val originalArguments = queueDeclaration.arguments
      val newArguments = originalArguments ++ arguments
      queueDeclaration.withArguments(newArguments)
    }

    def withDeadLetterExchange(deadLetterExchangeName: String): QueueDeclaration = {
      addArgument(deadLetterExchangeKey, deadLetterExchangeName)
    }

    def withDeadLetterRoutingKey(deadLetterRoutingKey: String): QueueDeclaration = {
      addArgument(deadLetterRoutingKeyKey, deadLetterRoutingKey)
    }
  }
}

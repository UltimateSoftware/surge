// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.surge.rabbit

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

    def withDeadLetterExchange(deadLetterExchangeName: String): QueueDeclaration = {
      addArgument(deadLetterExchangeKey, deadLetterExchangeName)
    }

    def withDeadLetterRoutingKey(deadLetterRoutingKey: String): QueueDeclaration = {
      addArgument(deadLetterRoutingKeyKey, deadLetterRoutingKey)
    }
  }
}

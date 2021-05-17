// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.stream.alpakka.amqp.{ BindingDeclaration, Declaration, ExchangeDeclaration, QueueDeclaration }

sealed trait Declarable {
  def declaration(): Declaration
}

class IllegalPlanException(message: String)
    extends RuntimeException(message)

    /**
     * Plan for queue declaration
     * @param name
     *   String
     * @param exclusive
     *   Boolean
     * @param durable
     *   Boolean
     * @param autoDelete
     *   Boolean
     * @param arguments
     *   Map
     */
case class QueuePlan(name: String, exclusive: Boolean, durable: Boolean, autoDelete: Boolean, arguments: Map[String, AnyRef] = Map.empty) extends Declarable {
  override def declaration(): Declaration = {
    QueueDeclaration(name).withDurable(durable).withExclusive(exclusive).withAutoDelete(autoDelete).withArguments(arguments)
  }
}

/**
 * Plan for Exchange Declaration
 * @param name
 *   String
 * @param durable
 *   Boolean
 * @param autoDelete
 *   Boolean
 * @param arguments
 *   Map
 */
case class ExchangePlan(name: String, durable: Boolean, autoDelete: Boolean, arguments: Map[String, AnyRef] = Map.empty) extends Declarable {
  override def declaration(): Declaration = {
    ExchangeDeclaration(name, "topic").withDurable(durable).withAutoDelete(autoDelete).withArguments(arguments)
  }
}

/**
 * Binding
 * @param queueName
 *   String
 * @param exchangeName
 *   String
 * @param routingKey
 *   Option[String]
 */
case class Binding(queueName: String, exchangeName: String, routingKey: Option[String]) extends Declarable {
  override def declaration(): Declaration = {
    routingKey match {
      case Some(key) =>
        BindingDeclaration(queueName, exchangeName).withRoutingKey(key);
      case None =>
        BindingDeclaration(queueName, exchangeName)
    }
  }
}

/**
 * AutoDeclarePlan
 * @param queuePlan
 *   QueuePlan
 * @param exchangePlan
 *   ExchangePlan
 * @param binding
 *   Binding
 */
case class AutoDeclarePlan(queuePlan: QueuePlan, exchangePlan: ExchangePlan, binding: Binding) {

  @throws[IllegalPlanException]
  def validate(): AutoDeclarePlan = {
    if (queuePlan.name != binding.queueName) {
      throw new IllegalPlanException("QueuePlan.name must match Binding.queueName")
    }

    if (exchangePlan.name != binding.exchangeName) {
      throw new IllegalPlanException("ExchangePlan.name must match Binding.exchangeName")
    }
    this
  }

  def declarations(): Seq[Declaration] = {
    val queueDeclaration = validate().queuePlan.declaration()
    val exchangeDeclaration = validate().exchangePlan.declaration()
    val bindingDeclaration = validate().binding.declaration()

    Seq(queueDeclaration, exchangeDeclaration, bindingDeclaration)
  }
}

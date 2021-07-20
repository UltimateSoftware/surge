// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.command

import io.opentelemetry.api.trace.Tracer
import surge.core._
import surge.kafka.KafkaTopic
import surge.scaladsl.command.{ AggregateCommandModel, SurgeCommandBusinessLogic }

import java.util.UUID

// #surge_model_class
// format: off
object BankAccountSurgeModelWithTracer
  extends SurgeCommandBusinessLogic[UUID, BankAccount, BankAccountCommand, BankAccountEvent] {

  override def tracer: Tracer = ???

  override def commandModel: AggregateCommandModel[BankAccount, BankAccountCommand, BankAccountEvent]
   = BankAccountCommandModel

  override def aggregateName: String = "bank-account"

  override def stateTopic: KafkaTopic = KafkaTopic("bank-account-state")

  override def eventsTopic: KafkaTopic = KafkaTopic("bank-account-events")

  override def aggregateReadFormatting: SurgeAggregateReadFormatting[BankAccount] = ???

  override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[BankAccount] = ???

  override def eventWriteFormatting: SurgeEventWriteFormatting[BankAccountEvent] = ???

}
// #surge_model_class
// format: on

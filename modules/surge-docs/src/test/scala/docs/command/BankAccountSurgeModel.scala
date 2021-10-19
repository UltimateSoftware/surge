// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.command

import play.api.libs.json._
import surge.core._
import surge.kafka.KafkaTopic
import surge.scaladsl.command.{ AggregateCommandModel, SurgeCommandBusinessLogic }

import java.util.UUID

// #surge_model_class
object BankAccountSurgeModel extends SurgeCommandBusinessLogic[UUID, BankAccount, BankAccountCommand, BankAccountEvent] {
  override def commandModel: AggregateCommandModel[BankAccount, BankAccountCommand, BankAccountEvent] = BankAccountCommandModel

  override def aggregateName: String = "bank-account"

  override def stateTopic: KafkaTopic = KafkaTopic("bank-account-state")

  override def eventsTopic: KafkaTopic = KafkaTopic("bank-account-events")

  override def aggregateReadFormatting: SurgeAggregateReadFormatting[BankAccount] = { (bytes: Array[Byte]) =>
    Json.parse(bytes).asOpt[BankAccount]
  }

  override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[BankAccount] = (agg: BankAccount) => {
    SerializedAggregate(Json.toJson(agg).toString().getBytes(), Map("aggregate_id" -> agg.accountNumber.toString))
  }

  override def eventWriteFormatting: SurgeEventWriteFormatting[BankAccountEvent] = (evt: BankAccountEvent) => {
    SerializedMessage(evt.accountNumber.toString, Json.toJson(evt)(Json.format[BankAccountEvent]).toString().getBytes())
  }
}
// #surge_model_class

package com.example.model

import play.api.libs.json.Json
import surge.core.{SerializedAggregate, SerializedMessage, SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting, SurgeEventWriteFormatting}
import surge.kafka.KafkaTopic
import surge.scaladsl.command.{AggregateCommandModel, SurgeCommandBusinessLogic}
import com.example.account.BankAccount
import com.example.command._
import com.example.event._

import java.util.UUID

object BankAccountSurgeModel extends SurgeCommandBusinessLogic[UUID, BankAccount, BankAccountCommand, BankAccountEvent] {
  override def commandModel: AggregateCommandModel[BankAccount, BankAccountCommand, BankAccountEvent] = BankAccountCommandModel

  override def aggregateName: String = "bank-account"

  override def stateTopic: KafkaTopic = KafkaTopic("bank-account-state")

  override def eventsTopic: KafkaTopic = KafkaTopic("bank-account-events")

  override def aggregateReadFormatting: SurgeAggregateReadFormatting[BankAccount] = (bytes: Array[Byte]) => Json.parse(bytes).asOpt[BankAccount]

  override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[BankAccount] = (agg: BankAccount) => {
    val aggBytes = Json.toJson(agg).toString().getBytes()
    val messageHeaders = Map("aggregate_id" -> agg.accountNumber.toString)
    SerializedAggregate(aggBytes, messageHeaders)
  }

  override def eventWriteFormatting: SurgeEventWriteFormatting[BankAccountEvent] = (evt: BankAccountEvent) => {
    val evtKey = evt.accountNumber.toString
    val evtBytes = evt.toJson.toString().getBytes()
    val messageHeaders = Map("aggregate_id" -> evt.accountNumber.toString)
    SerializedMessage(evtKey, evtBytes, messageHeaders)
  }
}
// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.command

import play.api.libs.json._
import surge.core._
import surge.kafka.KafkaTopic
import surge.scaladsl.command.{AggregateCommandModel, SurgeCommandBusinessLogic}
import surge.serialization.{Deserializer, PlayJsonSerializer, Serializer}

import java.util.UUID

// #surge_model_class
object BankAccountSurgeModel extends SurgeCommandBusinessLogic[UUID, BankAccount, BankAccountCommand, BankAccountEvent] {
  override def commandModel: AggregateCommandModel[BankAccount, BankAccountCommand, BankAccountEvent] = BankAccountCommandModel

  override def aggregateName: String = "bank-account"

  override def stateTopic: KafkaTopic = KafkaTopic("bank-account-state")

  override def eventsTopic: KafkaTopic = KafkaTopic("bank-account-events")

  override def aggregateReadFormatting: SurgeAggregateReadFormatting[BankAccount] = new SurgeAggregateReadFormatting[BankAccount] {
    override def readState(bytes: Array[Byte]): Option[BankAccount] =
      Some(stateDeserializer().deserialize(bytes))

    override def stateDeserializer(): Deserializer[BankAccount] =
      (body: Array[Byte]) => Json.parse(body).as[BankAccount]
  }

  override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[BankAccount] = new SurgeAggregateWriteFormatting[BankAccount] {
    override def writeState(agg: BankAccount): SerializedAggregate = {
      val bytesPlusHeaders = stateSerializer().serialize(agg)
      val messageHeaders = Map("aggregate_id" -> agg.accountNumber.toString)
      SerializedAggregate(bytesPlusHeaders.bytes, bytesPlusHeaders.headers)
    }

    override def stateSerializer(): Serializer[BankAccount] =
      new PlayJsonSerializer[BankAccount]()
  }

  override def eventWriteFormatting: SurgeEventWriteFormatting[BankAccountEvent] = new SurgeEventWriteFormatting[BankAccountEvent] {
    override def writeEvent(evt: BankAccountEvent): SerializedMessage = {
      val evtKey = evt.accountNumber.toString
      val evtBytes = evt.toJson.toString().getBytes()
      val messageHeaders = Map("aggregate_id" -> evt.accountNumber.toString)
      SerializedMessage(evtKey, evtBytes, messageHeaders)
    }

    override def eventSerializer(): Serializer[BankAccountEvent] =
      new PlayJsonSerializer[BankAccountEvent]()(Json.format[BankAccountEvent])
  }
}
// #surge_model_class

package com.example.event

import play.api.libs.json.{Format, JsValue, Json}

import java.util.UUID

sealed trait BankAccountEvent {
  def accountNumber: UUID
  def toJson: JsValue
}

object BankAccountCreated {
  implicit val format: Format[BankAccountCreated] = Json.format
}
case class BankAccountCreated(accountNumber: UUID, accountOwner: String, securityCode: String, balance: Double) extends BankAccountEvent {
  override def toJson: JsValue = Json.toJson(this)
}

object BankAccountUpdated {
  implicit val format: Format[BankAccountUpdated] = Json.format
}
case class BankAccountUpdated(accountNumber: UUID, newBalance: Double) extends BankAccountEvent {
  override def toJson: JsValue = Json.toJson(this)
}
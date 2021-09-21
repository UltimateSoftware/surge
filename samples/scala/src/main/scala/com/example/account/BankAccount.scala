// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.example.account

import play.api.libs.json.{Format, Json}

import java.util.UUID

object BankAccount {
  implicit val format: Format[BankAccount] = Json.format
}

case class BankAccount(accountNumber: UUID, accountOwner: String, securityCode: String, balance: Double)
package com.example.http.serializer

import com.example.http.request.{CreateAccountRequest, CreditAccountRequest, DebitAccountRequest}
import play.api.libs.json.Json

trait BankAccountRequestSerializer {
  implicit val createAccountFormat = Json.format[CreateAccountRequest]
  implicit val creditAccountFormat = Json.format[CreditAccountRequest]
  implicit val debitAccountFormat = Json.format[DebitAccountRequest]
}

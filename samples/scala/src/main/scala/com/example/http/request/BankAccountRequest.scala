package com.example.http.request

import com.example.command.{CreateAccount, CreditAccount, DebitAccount}

import java.util.UUID

case class CreateAccountRequest(accountOwner: String, securityCode: String, initialBalance: Double)
case class CreditAccountRequest(accountNumber: UUID, amount: Double)
case class DebitAccountRequest(accountNumber: UUID, amount: Double)

object RequestToCommand {
  def requestToCommand(request: CreateAccountRequest): CreateAccount = {
    val newAccountNumber = UUID.randomUUID()
    CreateAccount(newAccountNumber, request.accountOwner, request.securityCode, request.initialBalance)
  }

  def requestToCommand(request: CreditAccountRequest): CreditAccount = {
    CreditAccount(request.accountNumber, request.amount)
  }

  def requestToCommand(request: DebitAccountRequest): DebitAccount = {
    DebitAccount(request.accountNumber, request.amount)
  }
}

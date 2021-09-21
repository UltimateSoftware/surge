package com.example.command

import java.util.UUID

sealed trait BankAccountCommand {
  def accountNumber: UUID
}

case class CreateAccount(accountNumber: UUID, accountOwner: String, securityCode: String, initialBalance: Double) extends BankAccountCommand
case class CreditAccount(accountNumber: UUID, amount: Double) extends BankAccountCommand
case class DebitAccount(accountNumber: UUID, amount: Double)  extends BankAccountCommand

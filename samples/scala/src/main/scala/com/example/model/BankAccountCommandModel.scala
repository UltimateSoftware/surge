package com.example.model

import surge.scaladsl.command.AggregateCommandModel
import scala.util.{Failure, Success, Try}
import com.example.account.BankAccount
import com.example.command._
import com.example.event._
import com.example.exception._

object BankAccountCommandModel extends AggregateCommandModel[BankAccount, BankAccountCommand, BankAccountEvent] {
  override def processCommand(aggregate: Option[BankAccount], command: BankAccountCommand): Try[Seq[BankAccountEvent]] = {
    command match {
      case create: CreateAccount =>
        if (aggregate.isDefined) {
          // Aggregate already exists - no need to recreate
          Success(Seq.empty)
        } else {
          Success(Seq(BankAccountCreated(create.accountNumber, create.accountOwner, create.securityCode, create.initialBalance)))
        }
      case credit: CreditAccount =>
        aggregate
          .map { existing =>
            Success(Seq(BankAccountUpdated(existing.accountNumber, existing.balance + credit.amount)))
          }
          .getOrElse(Failure(new AccountDoesNotExistException(command.accountNumber)))
      case debit: DebitAccount =>
        aggregate
          .map { existing =>
            if (existing.balance >= debit.amount) {
              Success(Seq(BankAccountUpdated(existing.accountNumber, existing.balance - debit.amount)))
            } else {
              Failure(new InsufficientFundsException(existing.accountNumber))
            }
          }
          .getOrElse(Failure(new AccountDoesNotExistException(command.accountNumber)))
    }
  }

  override def handleEvent(aggregate: Option[BankAccount], event: BankAccountEvent): Option[BankAccount] = {
    event match {
      case create: BankAccountCreated  => Some(BankAccount(create.accountNumber, create.accountOwner, create.securityCode, create.balance))
      case updated: BankAccountUpdated => aggregate.map(_.copy(balance = updated.newBalance))
    }
  }
}

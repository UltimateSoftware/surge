// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.command

import java.util.UUID

import play.api.libs.json.{ Format, JsValue, Json }
import surge.scaladsl.command.AggregateCommandModel

import scala.util.{ Failure, Success, Try }

class InsufficientFundsException(accountNumber: UUID) extends RuntimeException(s"Insufficient Funds in account $accountNumber to complete this transaction")
class AccountDoesNotExistException(accountNumber: UUID)
    extends RuntimeException(s"Account with id $accountNumber does not exist")

// #aggregate_class
object BankAccount {
  implicit val format: Format[BankAccount] = Json.format
}
case class BankAccount(accountNumber: UUID, accountOwner: String, securityCode: String, balance: Double)
// #aggregate_class

// #command_class
sealed trait BankAccountCommand {
  def accountNumber: UUID
}
case class CreateAccount(accountNumber: UUID, accountOwner: String, securityCode: String, initialBalance: Double) extends BankAccountCommand
case class CreditAccount(accountNumber: UUID, amount: Double) extends BankAccountCommand
case class DebitAccount(accountNumber: UUID, amount: Double)
    extends BankAccountCommand
// #command_class

// #event_class
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
// #event_class

// #command_model_class
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
// #command_model_class

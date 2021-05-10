// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.command

import surge.scaladsl.common.{ CommandFailure, CommandSuccess }

import scala.concurrent.{ ExecutionContext, Future }

// #bank_account_service_class
class BankAccountService {
  def handleBankAccountCommand(command: BankAccountCommand)(implicit ec: ExecutionContext): Future[Option[BankAccount]] = {
    BankAccountEngine.surgeEngine.aggregateFor(command.accountNumber).sendCommand(command).map {
      case CommandSuccess(aggregateState) => aggregateState
      case CommandFailure(reason)         => throw reason
    }
  }
}
// #bank_account_service_class

// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.command

import java.util.UUID

import surge.scaladsl.command.SurgeCommand

// #bank_account_engine_class
object BankAccountEngine {
  lazy val surgeEngine: SurgeCommand[UUID, BankAccount, BankAccountCommand, Nothing, BankAccountEvent] = {
    val engine = SurgeCommand(BankAccountSurgeModel)
    engine.start()
    engine
  }
}
// #bank_account_engine_class

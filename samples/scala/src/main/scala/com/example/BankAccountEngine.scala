package com.example

import com.example.account.BankAccount
import com.example.command.BankAccountCommand
import com.example.event.BankAccountEvent
import com.example.model.BankAccountSurgeModel
import surge.scaladsl.command.SurgeCommand

import java.util.UUID

object BankAccountEngine {
  lazy val surgeEngine: SurgeCommand[UUID, BankAccount, BankAccountCommand, Nothing, BankAccountEvent] = {
    val engine = SurgeCommand(BankAccountSurgeModel)
    engine.start()
    engine
  }
}
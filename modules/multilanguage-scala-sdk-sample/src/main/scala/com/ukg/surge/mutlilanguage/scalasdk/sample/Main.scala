// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package com.ukg.surge.multilanguage.scalasdk.sample

import com.ukg.surge.multilanguage.scalasdk.CQRSModel

object Main extends App {

  case class BankAccount(balance: Int)

  case class MoneyDeposited(amount: Int)

  case class DepositMoney(amount: Int)

  def eventHandler(agg: Option[BankAccount], evt: MoneyDeposited): Option[BankAccount] = {
    (agg, evt) match {
      case (None, MoneyDeposited(amount)) => Some(BankAccount(amount))
      case (Some(BankAccount(balance)), MoneyDeposited(amount)) => Some(BankAccount(balance + amount))
    }
  }

  def commandHandler(agg: Option[BankAccount], cmd: DepositMoney): Either[String, Seq[MoneyDeposited]] = {
    agg match {
      case Some(BankAccount(balance)) => if (cmd.amount >)
      case None => None
    }
  }

  val model = CQRSModel[BankAccount, MoneyDeposited, DepositMoney](
    eventHandler, commandHandler
  )

}
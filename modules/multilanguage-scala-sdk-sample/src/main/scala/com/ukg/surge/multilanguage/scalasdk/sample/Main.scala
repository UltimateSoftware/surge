// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage.scalasdk.sample

import akka.actor.ActorSystem
import akka.event.Logging
import com.ukg.surge.multilanguage.scalasdk.{ CQRSModel, ScalaSurge, ScalaSurgeServer, SerDeser }

import scala.util.Try

class Main
object Main extends App {

  case class BankAccount(balance: Int)

  case class MoneyDeposited(amount: Int)

  case class DepositMoney(amount: Int)

  def eventHandler(agg: Option[BankAccount], evt: MoneyDeposited): Option[BankAccount] = {
    (agg, evt) match {
      case (None, MoneyDeposited(amount))                       => Some(BankAccount(amount))
      case (Some(BankAccount(balance)), MoneyDeposited(amount)) => Some(BankAccount(balance + amount))
    }
  }

  def commandHandler(agg: Option[BankAccount], cmd: DepositMoney): Either[String, Seq[MoneyDeposited]] = {
    if (cmd.amount >= 0) {
      Right(MoneyDeposited(cmd.amount) :: Nil)
    } else {
      Left("Amount cannot be < 0")
    }
  }

  val model = CQRSModel[BankAccount, MoneyDeposited, DepositMoney](eventHandler, commandHandler)

  val serDeser = {
    import org.json4s._
    import org.json4s.native.Serialization._
    import org.json4s.native.Serialization.read
    implicit val formats: DefaultFormats.type = DefaultFormats

    def deserialize[T](bytes: Array[Byte])(implicit m: Manifest[T]): Try[T] = {
      Try {
        val jsonString = new String(bytes)
        read[T](jsonString)
      }
    }

    def serialize[T](t: T)(implicit m: Manifest[T]): Try[Array[Byte]] = {
      Try {
        write[T](t).getBytes()
      }
    }

    def deserializeState(bytes: Array[Byte]): Try[BankAccount] = deserialize[BankAccount](bytes)
    def deserializeEvent(bytes: Array[Byte]): Try[MoneyDeposited] = deserialize[MoneyDeposited](bytes)
    def deserializeCommand(bytes: Array[Byte]): Try[DepositMoney] = deserialize[DepositMoney](bytes)

    new SerDeser[BankAccount, MoneyDeposited, DepositMoney](
      deserializeState,
      deserializeEvent,
      deserializeCommand,
      serialize[BankAccount],
      serialize[MoneyDeposited],
      serialize[DepositMoney])

  }

  val system = ActorSystem()
  val logger = Logging(system, classOf[Main])
  val bridgeToSurge = new ScalaSurgeServer[BankAccount, MoneyDeposited, DepositMoney](system, model, serDeser)
  logger.info("Started!")

}

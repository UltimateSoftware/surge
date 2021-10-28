// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage.scalasdk.sample

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.server.Directives._
import com.ukg.surge.multilanguage.scalasdk.{ CQRSModel, ScalaSurgeServer, SerDeser }
import org.json4s._
import org.json4s.native.Serialization._
import java.util.UUID
import scala.util.{ Failure, Success, Try }

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

    implicit val formats: DefaultFormats.type = DefaultFormats

    def deserialize[T](bytes: Array[Byte])(implicit m: Manifest[T]): Try[T] =
      Try(read[T](new String(bytes)))

    def serialize[T](t: T)(implicit m: Manifest[T]): Try[Array[Byte]] =
      Try(write[T](t).getBytes())

    new SerDeser[BankAccount, MoneyDeposited, DepositMoney](
      deserialize[BankAccount],
      deserialize[MoneyDeposited],
      deserialize[DepositMoney],
      serialize[BankAccount],
      serialize[MoneyDeposited],
      serialize[DepositMoney])

  }

  implicit val system = ActorSystem()
  val host = system.settings.config.getString("host")
  val logger = Logging(system, classOf[Main])
  val bridgeToSurge = new ScalaSurgeServer[BankAccount, MoneyDeposited, DepositMoney](system, model, serDeser)
  logger.info("Started!")

  val route =
    path("deposit" / JavaUUID / IntNumber) { (uuid: UUID, amount: Int) =>
      get {
        onComplete(bridgeToSurge.forwardCommand(uuid, DepositMoney(amount))) {
          case Success(value) => complete(s"The result is $value \n")
          case Failure(ex)    => complete(InternalServerError, s"An error occurred: ${ex.getMessage}\n")
        }
      }
    } ~ path("state" / JavaUUID) { uuid =>
      {
        onComplete(bridgeToSurge.getState(uuid)) {
          case Success(value) => complete(s"The result is $value \n")
          case Failure(ex)    => complete(InternalServerError, s"An error occurred: ${ex.getMessage}\n")
        }
      }
    }

  val bindingFuture = Http().newServerAt(host, 8080).bind(route)

}

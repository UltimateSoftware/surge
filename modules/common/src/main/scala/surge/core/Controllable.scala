// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import scala.concurrent.Future

//final case class ControlAck(success: Boolean, error: Option[Throwable] = None) extends Ack {
//  override def withSuccess(success: Boolean): Ack = {
//    copy(success = success)
//  }
//
////  override def withError(error: Throwable): Ack = {
////    copy(error = Some(error))
////  }
//}

final case class Ack()

trait ControllableLookup {
  def lookup(identifier: String): Option[Controllable]
}

trait ControllableRemover {
  def remove(identifier: String): Option[Controllable]
}

trait Controllable {
  def start(): Future[Ack]
  def restart(): Future[Ack]
  def stop(): Future[Ack]
  def shutdown(): Future[Ack]
}

class ControllableAdapter extends Controllable {

  override def start(): Future[Ack] = Future.successful[Ack](Ack())

  override def restart(): Future[Ack] = Future.successful[Ack](Ack())

  override def stop(): Future[Ack] = Future.successful[Ack](Ack())

  override def shutdown(): Future[Ack] = Future.successful(Ack())
}

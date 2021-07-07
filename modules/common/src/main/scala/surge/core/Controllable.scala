// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import scala.concurrent.Future

case class ControlAck(success: Boolean, details: Map[String, Any] = Map.empty, error: Option[Throwable] = None)

trait Controllable {
  def start(): Future[ControlAck]
  def restart(): Future[ControlAck]
  def stop(): Future[ControlAck]
  def shutdown(): Future[ControlAck]
}

class ControllableAdapter extends Controllable {
  override def start(): Future[ControlAck] = Future.successful(ControlAck(success = true))

  override def restart(): Future[ControlAck] = Future.successful(ControlAck(success = true))

  override def stop(): Future[ControlAck] = Future.successful(ControlAck(success = true))

  override def shutdown(): Future[ControlAck] = Future.successful(ControlAck(success = true))
}

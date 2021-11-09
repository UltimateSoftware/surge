// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import scala.concurrent.Future

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
  def stopInternal(): Future[Ack]
  def shutdown(): Future[Ack]
}

class ControllableAdapter extends Controllable {

  override def start(): Future[Ack] = Future.successful[Ack](Ack())

  override def restart(): Future[Ack] = Future.successful[Ack](Ack())

  override def stopInternal(): Future[Ack] = Future.successful[Ack](Ack())

  override def shutdown(): Future[Ack] = Future.successful(Ack())
}

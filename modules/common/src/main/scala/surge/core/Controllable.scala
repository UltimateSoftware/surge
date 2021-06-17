// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

trait Controllable {
  def start(): Unit
  def restart(): Unit
  def stop(): Unit
  def shutdown(): Unit
}

class ControllableAdapter extends Controllable {
  override def start(): Unit = {}

  override def restart(): Unit = {}

  override def stop(): Unit = {}

  override def shutdown(): Unit = {}
}

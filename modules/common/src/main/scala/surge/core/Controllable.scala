// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import scala.collection.mutable

trait Controllable {
  def start(): Unit
  def stop(): Unit
  def restart(): Unit
  def shutdown(): Unit
}

trait ControllableWithHooks extends Controllable {
  type ShutdownHook = Controllable => Unit
  type RestartHook = Controllable => Unit
  def restart(): Unit = {
    handleRestart()
  }

  def shutdown(): Unit = {
    handleShutdown()
  }

  def hasShutdownHooks: Boolean =
    shutdownHooks.nonEmpty

  def hasRestartHooks: Boolean =
    restartHooks.nonEmpty

  def onShutdown(shutdownHook: ShutdownHook): Unit =
    shutdownHooks.add(shutdownHook)

  def onRestart(restartHook: RestartHook): Unit =
    restartHooks.add(restartHook)

  private def handleShutdown(): Unit = {
    shutdownHooks.foreach(doIt => doIt(this))
  }

  private def handleRestart(): Unit = {
    restartHooks.foreach(doIt => doIt(this))
  }

  private val shutdownHooks: mutable.Set[ShutdownHook] = mutable.Set.empty
  private val restartHooks: mutable.Set[RestartHook] = mutable.Set.empty
}

class ControllableAdapter extends ControllableWithHooks {
  override def start(): Unit = {}

  override def stop(): Unit = {}
}

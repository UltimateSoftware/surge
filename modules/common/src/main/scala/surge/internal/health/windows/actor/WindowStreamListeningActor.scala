// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.actor

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import org.slf4j.{ Logger, LoggerFactory }
import surge.health.domain.HealthSignal
import surge.health.windows.{ AddedToWindow, Window, WindowAdvanced, WindowClosed, WindowOpened, WindowStopped, WindowStreamListener }

object WindowStreamListeningActorAdapter {
  val log: Logger = LoggerFactory.getLogger(getClass)
  def apply(actorSystem: ActorSystem, actorRefOverride: Option[ActorRef] = None): WindowStreamListeningActorRef = {
    // if actorRefOverride not provided; return WindowStreamListeningActorRef with a
    //  default WindowStreamListeningActorAdapter actorRef.
    val ref: ActorRef = actorRefOverride.getOrElse[ActorRef](actorSystem.actorOf(Props(new WindowStreamListeningActorAdapter())))
    WindowStreamListeningActorRef(ref)
  }
}

case class WindowStreamListeningActorRef(actor: ActorRef)

private class WindowStreamListeningActorAdapter extends Actor with WindowStreamListener {
  override def receive: Receive = {
    case WindowOpened(w) =>
      windowOpened(w)
    case WindowAdvanced(w, data) =>
      windowAdvanced(w, data.signals)
    case WindowClosed(w, data) =>
      windowClosed(w, data.signals)
    case WindowStopped(w) =>
      windowStopped(w)
    case AddedToWindow(e, w) =>
      dataAddedToWindow(e, w)
  }

  override def windowOpened(window: Window): Unit = {}

  override def windowClosed(window: Window, data: Seq[HealthSignal]): Unit = {}

  override def windowAdvanced(window: Window, data: Seq[HealthSignal]): Unit = {}

  override def windowStopped(window: Option[Window]): Unit = {}

  override def dataAddedToWindow(dat: HealthSignal, window: Window): Unit = {}
}

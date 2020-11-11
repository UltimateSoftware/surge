// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.support

import akka.actor.Actor.Receive

trait Exiter {
  def exit(exitCode: Int): Unit
}
object SystemExit extends Exiter {
  override def exit(exitCode: Int): Unit = sys.exit(exitCode)
}

/**
 * Little helper to add type to the inline Receive functions
 * compiler was complaining about not knowing the type of inline partial function
 * when use multiple Receive functions like
 * def receive: Receive = receiveFunction1 orElse { case MyCommand => ... }
 * This helper enable us to do like
 * def receive: Receive = receiveFunction1 orElse inlineReceive { case MyCommand => ... }
 */
object inlineReceive {
  def apply(f: PartialFunction[Any, Unit]): Receive = f
}

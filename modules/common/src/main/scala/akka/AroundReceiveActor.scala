// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package akka

import akka.actor.Actor

/**
 * Actor that can be used to extend the underlying behavior of aroundReceive in order to extend or transform messages before they're passed to an actor.
 * Be careful when using this as the power exposed leaves a lot of room to shoot yourself in the foot.
 */
trait AroundReceiveActor extends Actor {
  override protected[akka] def aroundReceive(receive: Receive, msg: Any): Unit = {
    doAroundReceive(receive, msg)
    afterReceive(receive, msg)
  }

  def superAroundReceive(receive: Receive, msg: Any): Unit = {
    super.aroundReceive(receive, msg)
  }
  def doAroundReceive(receive: Receive, msg: Any): Unit = superAroundReceive(receive, msg)
  def afterReceive(receive: Receive, msg: Any): Unit = {}
}

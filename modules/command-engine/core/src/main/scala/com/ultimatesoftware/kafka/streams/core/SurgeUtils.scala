// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

object SurgeUtils {

  /**
   * The Akka ActorSystem only likes to be named with
   * @param initialName
   * @return
   */
  def standardizeActorSystemName(initialName: String): String = {
    initialName
  }

}

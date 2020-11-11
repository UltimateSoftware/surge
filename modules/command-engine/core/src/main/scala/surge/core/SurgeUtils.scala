// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

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

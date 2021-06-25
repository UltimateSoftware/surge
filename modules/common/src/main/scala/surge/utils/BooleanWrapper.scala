// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.utils

import scala.collection.mutable

class BooleanWrapper {
  private val holder: mutable.Map[String, Boolean] = mutable.Map[String, Boolean]("holder" -> false)

  def setFalse(): Unit = {
    holder.put("holder", false)
  }

  def setTrue(): Unit = {
    holder.put("holder", true)
  }

  def asTrue(): BooleanWrapper = {
    val copy = this.copy()
    copy.setTrue()
    copy
  }

  def asFalse(): BooleanWrapper = {
    val copy = this.copy()
    copy.setFalse()
    copy
  }

  def copy(): BooleanWrapper = {
    if (wrapped()) {
      val copy = new BooleanWrapper()
      copy.setFalse()
      copy
    } else {
      val copy = new BooleanWrapper()
      copy.setTrue()
      copy
    }
  }

  def wrapped(): Boolean = holder.getOrElse("holder", false)
}

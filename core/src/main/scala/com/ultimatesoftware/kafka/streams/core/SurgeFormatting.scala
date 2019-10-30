// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

private[streams] trait SurgeFormatting[Event] {
  def writeEvent(evt: Event): Array[Byte]
}

// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

private[streams] trait SurgeFormatting[Event] {
  def writeEvent(evt: Event): Array[Byte]
}

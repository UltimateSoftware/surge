// Copyright (C) 2018 Ultimate Software

package com.ultimatesoftware.kafka.streams.core

import play.api.libs.json.Format

// TODO there has to be a better way to do this
trait SurgeFormatting[Command, Event] {
  implicit def commandFormat: Format[Command]
  implicit def eventFormat: Format[Event]
}

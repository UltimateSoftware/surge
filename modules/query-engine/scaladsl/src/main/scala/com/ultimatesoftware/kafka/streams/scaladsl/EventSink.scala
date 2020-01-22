// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.scaladsl

import com.ultimatesoftware.kafka.streams.core
import com.ultimatesoftware.scala.core.messaging.EventProperties

trait EventSink[Event] extends core.EventSink[Event, EventProperties]

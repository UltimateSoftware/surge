// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.config

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object TimeoutConfig {
  private val config = ConfigFactory.load()

  private val debugTimeoutEnabled = config.getBoolean("surge.debug-mode-timeouts-enabled")

  private val timeoutScaleFactor = if (debugTimeoutEnabled) {
    1000
  } else {
    1
  }

  object AggregateActor {
    val idleTimeout: FiniteDuration =
      config.getDuration("surge.aggregate-actor.idle-timeout", TimeUnit.MILLISECONDS).milliseconds * timeoutScaleFactor
    val askTimeout: FiniteDuration =
      config.getDuration("surge.aggregate-actor.ask-timeout", TimeUnit.MILLISECONDS).milliseconds * timeoutScaleFactor
  }

  object ShardRouter {
    val askTimeout: FiniteDuration = 7.seconds * timeoutScaleFactor
  }

  object PartitionTracker {
    val updateTimeout: FiniteDuration = 20.seconds * timeoutScaleFactor
  }

  object PublisherActor {
    val publishTimeout: FiniteDuration =
      config.getDuration("surge.producer.publish-timeout", TimeUnit.MILLISECONDS).milliseconds * timeoutScaleFactor
    val aggregateStateCurrentTimeout: FiniteDuration = 10.seconds * timeoutScaleFactor
  }

}

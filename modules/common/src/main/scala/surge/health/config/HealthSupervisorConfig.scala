// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.config

import com.typesafe.config.ConfigFactory

object HealthSupervisorConfig {
  private val config = ConfigFactory.load()
  val jmxEnabled: Boolean = config.getBoolean("surge.health.supervisor-actor.jmx-enabled")

  def apply(): HealthSupervisorConfig = HealthSupervisorConfig(jmxEnabled)
}

case class HealthSupervisorConfig(jmxEnabled: Boolean)

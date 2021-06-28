// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.config

import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * WindowStreamConfig encapsulates all the configuration options for a WindowingHealthSignalStream
 */
case class WindowingStreamConfig(maxDelay: FiniteDuration, maxStreamSize: Int, frequencies: Seq[FiniteDuration], advancerConfig: WindowingStreamAdvancerConfig)

trait WindowingStreamAdvancerConfigLoader[T] {
  def load(config: Config): T
}

sealed trait WindowingStreamAdvancerConfig {
  def advanceAmount: Int
  def buffer: Int
}

case class WindowingStreamSliderConfig(buffer: Int, advanceAmount: Int) extends WindowingStreamAdvancerConfig

object WindowingStreamAdvancerConfigLoader {
  def apply(advancerType: String): WindowingStreamAdvancerConfigLoader[WindowingStreamSliderConfig] = {
    WindowingStreamSliderConfigLoader
  }
}

object WindowingStreamSliderConfigLoader extends WindowingStreamAdvancerConfigLoader[WindowingStreamSliderConfig] {
  def load(advancerConfig: Config): WindowingStreamSliderConfig = {
    val configuredSlideAmount: Int = advancerConfig.getInt("amount")
    val configuredBufferSize: Int = advancerConfig.getInt("buffer")

    WindowingStreamSliderConfig(configuredBufferSize, configuredSlideAmount)
  }
}

object WindowingStreamConfigLoader {
  private val config = ConfigFactory.load().getConfig("surge.health.window.stream")

  def load(config: Config): WindowingStreamConfig = {
    val maxDelay = config.getDuration("max-delay").toMillis.millis
    val maxStreamSize = config.getInt("max-size")
    val frequencies = config.getDurationList("frequencies").asScala.map(d => d.toMillis.millis)

    val advancerConfig = config.getConfig("advancer")
    val windowStreamAdvancerConfig = WindowingStreamAdvancerConfigLoader(advancerConfig.getString("type")).load(advancerConfig)

    WindowingStreamConfig(maxDelay, maxStreamSize, frequencies.toSeq, windowStreamAdvancerConfig)
  }

  def load(): WindowingStreamConfig = {
    load(config)
  }
}

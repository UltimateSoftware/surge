// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.config

import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * WindowStreamConfig encapsulates all the configuration options for a WindowingHealthSignalStream
 */
case class WindowingStreamConfig(
    maxDelay: FiniteDuration = 5.seconds,
    maxStreamSize: Int = 500,
    frequencies: Seq[FiniteDuration] = Seq(10.seconds),
    advancerConfig: WindowingStreamAdvancerConfig)

trait WindowingStreamAdvancerConfigLoader[T] {
  def load(config: Config): T
}

sealed trait WindowingStreamAdvancerConfig {
  def advanceAmount: Int
  def buffer: Int
}

case class WindowingStreamSliderConfig(buffer: Int = 10, advanceAmount: Int = 1) extends WindowingStreamAdvancerConfig

object WindowingStreamAdvancerConfigLoader {
  def apply(str: String): WindowingStreamAdvancerConfigLoader[WindowingStreamSliderConfig] = {
    WindowingStreamSliderConfigLoader
  }
}

object WindowingStreamSliderConfigLoader extends WindowingStreamAdvancerConfigLoader[WindowingStreamSliderConfig] {
  def load(advancerConfig: Config): WindowingStreamSliderConfig = {
    val configuredSlideAmount: Int = Try { advancerConfig.getInt("slider.amount") }.getOrElse(1)
    val configuredBufferSize: Int = Try { advancerConfig.getInt("slider.buffer") }.getOrElse(10)

    WindowingStreamSliderConfig(configuredBufferSize, configuredSlideAmount)
  }
}

object WindowingStreamConfigLoader {
  private val config = ConfigFactory.load()
  def load(): WindowingStreamConfig = {
    val maxDelay = FiniteDuration(config.getDuration("surge.health.window.stream.max-delay").toMillis, "millis")
    val maxStreamSize = config.getInt("surge.health.window.stream.max-size")
    val frequencies = config.getDurationList("surge.health.window.stream.frequencies").asScala.map(d => FiniteDuration(d.toMillis, "millis"))

    val advancerConfig = config.getConfig("surge.health.window.stream.advancer")
    val windowStreamAdvancerConfig = WindowingStreamAdvancerConfigLoader(advancerConfig.getString("type")).load(advancerConfig)
    WindowingStreamConfig(maxDelay, maxStreamSize, frequencies.toSeq, windowStreamAdvancerConfig)
  }
}

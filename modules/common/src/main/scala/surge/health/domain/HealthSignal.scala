// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.domain

import akka.actor.NoSerializationVerificationNeeded
import org.slf4j.{ Logger, LoggerFactory }

import java.time.Instant
import java.util.UUID
import surge.health.{ HealthMessage, SignalType }
import surge.internal.health.HealthSignalImpl

trait HealthSignalSource {
  def flush(): Unit
  def signals(): Seq[HealthSignal]
}

object SnapshotHealthSignalSource {
  val log: Logger = LoggerFactory.getLogger(getClass)
}

class SnapshotHealthSignalSource(private val data: Seq[HealthSignal]) extends HealthSignalSource {
  import SnapshotHealthSignalSource._
  def flush(): Unit = {
    log.warn("Flush not supported in SnapshotHealthSignalSource")
  }

  override def signals(): Seq[HealthSignal] = data
}

object HealthSignal {
  def apply(
      topic: String,
      name: String,
      signalType: SignalType.Value,
      data: SignalData,
      metadata: Map[String, String] = Map[String, String](),
      source: Option[HealthSignalSource],
      handled: Boolean = false,
      id: UUID = UUID.randomUUID(),
      timestamp: Instant = Instant.now()): HealthSignal = {
    HealthSignalImpl(topic, name, signalType, data, metadata, source, handled, id, timestamp)
  }
}

trait HealthSignal extends HealthMessage with NoSerializationVerificationNeeded {
  def topic(): String
  def name(): String
  def signalType: SignalType.Value
  def data: SignalData
  def metadata: Map[String, String] = Map.empty
  def source: Option[HealthSignalSource]
  def handled: Boolean = false

  def handled(h: Boolean): HealthSignal
  def data(d: SignalData): HealthSignal
  def source(s: Option[HealthSignalSource]): HealthSignal
}

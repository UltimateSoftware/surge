// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health.domain

import org.slf4j.{ Logger, LoggerFactory }
import akka.actor.NoSerializationVerificationNeeded

import java.time.Instant
import java.util.UUID
import surge.health.{ HealthMessage, SignalType }

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

case class HealthSignal(
    topic: String,
    name: String,
    signalType: SignalType.Value,
    data: SignalData,
    metadata: Map[String, String] = Map[String, String](),
    source: Option[HealthSignalSource],
    handled: Boolean = false,
    id: UUID = UUID.randomUUID(),
    timestamp: Instant = Instant.now())
    extends HealthMessage
    with NoSerializationVerificationNeeded
    with Timed

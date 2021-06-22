// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.domain

import java.time.Instant
import java.util.UUID
import java.util.regex.Pattern

import surge.core.Controllable
import surge.health.SignalType

sealed trait HealthMessage extends Timed {
  def topic(): String
  def id(): UUID
  def timestamp: Instant
}

case class HealthSignal(
    topic: String,
    name: String,
    signalType: SignalType.Value,
    data: SignalData,
    metadata: Map[String, String] = Map[String, String](),
    handled: Boolean = false,
    id: UUID = UUID.randomUUID(),
    timestamp: Instant = Instant.now())
    extends HealthMessage
    with Timed

case class HealthRegistration(
    control: Controllable,
    topic: String,
    name: String,
    restartSignalPatterns: Seq[Pattern] = Seq.empty,
    shutdownSignalPatterns: Seq[Pattern] = Seq.empty,
    id: UUID = UUID.randomUUID(),
    timestamp: Instant = Instant.now)
    extends HealthMessage
    with Timed

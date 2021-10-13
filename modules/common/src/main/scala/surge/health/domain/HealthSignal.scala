// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.domain

import akka.actor.NoSerializationVerificationNeeded

import java.time.Instant
import java.util.UUID
import surge.health.{ HealthMessage, SignalType }

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
    with NoSerializationVerificationNeeded
    with Timed

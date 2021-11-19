// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.domain

import akka.actor.{ ActorRef, NoSerializationVerificationNeeded }
import surge.core.Controllable
import surge.health.HealthMessage
import surge.internal.health.HealthRegistrationImpl

import java.time.Instant
import java.util.UUID
import java.util.regex.Pattern

object HealthRegistration {
  def apply(
      componentName: String,
      control: Controllable,
      topic: String,
      restartSignalPatterns: Seq[Pattern] = Seq.empty,
      shutdownSignalPatterns: Seq[Pattern] = Seq.empty,
      id: UUID = UUID.randomUUID(),
      timestamp: Instant = Instant.now,
      ref: Option[ActorRef] = None): HealthRegistration = {
    HealthRegistrationImpl(componentName, control, topic, restartSignalPatterns, shutdownSignalPatterns, id, timestamp, ref)
  }
}

trait HealthRegistration extends HealthMessage with NoSerializationVerificationNeeded {
  def componentName(): String
  def control(): Controllable
  def topic(): String
  def restartSignalPatterns(): Seq[Pattern] = Seq.empty
  def shutdownSignalPatterns(): Seq[Pattern] = Seq.empty
  def id(): UUID
  def timestamp: Instant
  def ref(): Option[ActorRef] = None
}

// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health.windows

import akka.actor.ActorRef

import java.time.Instant
import org.slf4j.{ Logger, LoggerFactory }
import surge.health.domain.{ HealthSignal, HealthSignalSource }
import surge.internal.health.windows.actor.HealthSignalWindowActor.Flush

import scala.concurrent.duration.FiniteDuration

object Window {
  val log: Logger = LoggerFactory.getLogger(getClass)
  def windowFor(ts: Instant, duration: FiniteDuration, control: Option[ActorRef]): Window = {
    Window(ts.toEpochMilli, ts.plusMillis(duration.toMillis).toEpochMilli, duration = duration, data = Seq.empty, control = control)
  }
}

case class WindowSnapShot(data: Seq[HealthSignal])
case class Window(from: Long, to: Long, data: Seq[HealthSignal], priorData: Seq[HealthSignal] = Seq.empty, duration: FiniteDuration, control: Option[ActorRef])
    extends HealthSignalSource {
  override def toString: String = s"At ${Instant.now().toEpochMilli} Window From $from To $to} - expired == ${expired()} - durationInMillis == $duration"

  def expired(): Boolean = {
    to <= Instant.now().toEpochMilli
  }

  def snapShotData(): Seq[HealthSignal] = {
    if (data.isEmpty) {
      priorData
    } else {
      data
    }
  }

  override def flush(): Unit = {
    control.foreach(a => a ! Flush())
  }

  override def signals(): Seq[HealthSignal] = data
}

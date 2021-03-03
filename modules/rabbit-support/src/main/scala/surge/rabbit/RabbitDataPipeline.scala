// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.amqp.scaladsl.CommittableReadResult
import akka.stream.scaladsl.{ Keep, RestartSource, Sink, Source }
import akka.stream.{ KillSwitch, KillSwitches, Materializer }
import surge.streams.DataPipeline
import surge.streams.DataPipeline.ReplaySuccessfullyStarted

import scala.concurrent.Future
import scala.concurrent.duration._

case class RestartBackoff(minBackoff: FiniteDuration, maxBackoff: FiniteDuration, randomFactor: Double)

object RabbitDataPipeline {
  val defaultRestartBackoff: RestartBackoff = RestartBackoff(2.seconds, 5.seconds, 0.2)
}

private[rabbit] class RabbitDataPipeline(
    source: Source[CommittableReadResult, NotUsed],
    actorSystem: ActorSystem,
    backoff: Option[RestartBackoff]) extends DataPipeline {
  import RabbitDataPipeline._
  private var killSwitch: KillSwitch = _

  override def start(): Unit = {
    val backoffToUse = backoff.getOrElse(defaultRestartBackoff)
    this.killSwitch = RestartSource.onFailuresWithBackoff(
      minBackoff = backoffToUse.minBackoff,
      maxBackoff = backoffToUse.maxBackoff,
      randomFactor = backoffToUse.randomFactor)(() => source).mapAsync(parallelism = 1)(cm => cm.ack(multiple = false))
      .viaMat(KillSwitches.single)(Keep.right).toMat(Sink.ignore)(Keep.left).run()(Materializer(actorSystem))
  }

  override def stop(): Unit = {
    Option(this.killSwitch).foreach(switch => switch.shutdown())
  }

  override def replay(): Future[DataPipeline.ReplayResult] = Future.successful(ReplaySuccessfullyStarted())
}

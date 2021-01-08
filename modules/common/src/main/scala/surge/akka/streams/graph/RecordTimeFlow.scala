// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.akka.streams.graph

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Flow
import surge.metrics.Timer

object RecordTimeFlow {
  def apply[T, Out, Mat](flow: Flow[T, Out, Mat], timer: Timer): Flow[T, Out, NotUsed] = Flow[T]
    .map(t => t -> Instant.now)
    .via(PassThroughFlow(Flow[(T, Instant)].map(_._1).via(flow)))
    .map {
      case (output, (_, startTime)) =>
        val timeElapsed = Instant.now.toEpochMilli - startTime.toEpochMilli
        timer.recordTime(timeElapsed)
        output
    }
}

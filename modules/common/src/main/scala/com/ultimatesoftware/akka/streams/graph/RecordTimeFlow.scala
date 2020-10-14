// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.graph

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.{ Flow, Keep }
import com.ultimatesoftware.scala.core.monitoring.metrics.Timer

object RecordTimeFlow {
  def apply[T, Out, Mat](flow: Flow[T, Out, Mat], timer: Timer): Flow[T, Out, NotUsed] = Flow[T]
    .map(t ⇒ t -> Instant.now)
    .via(PassThroughFlow(Flow[(T, Instant)].map(_._1).via(flow)))
    .map {
      case (output, (_, startTime)) ⇒
        val timeElapsed = Instant.now.toEpochMilli - startTime.toEpochMilli
        timer.recordTime(timeElapsed)
        output
    }
}

// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.graph

import akka.NotUsed
import akka.stream.{ FlowShape, Graph }
import akka.stream.scaladsl.{ Flow, GraphDSL, Merge, Partition }

object OptionFlow {
  def apply[In, Out](someFlow: Flow[In, Out, NotUsed], noneFlow: Flow[None.type, Out, NotUsed]): Graph[FlowShape[Option[In], Out], NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit builder ⇒
      import GraphDSL.Implicits._

      def partitioner(o: Option[In]): Int = o.map(_ ⇒ 0).getOrElse(1)
      val partition = builder.add(Partition[Option[In]](2, partitioner))
      val merge = builder.add(Merge[Out](2))

      partition.out(0) ~> Flow[Option[In]].collect { case Some(t) ⇒ t } ~> someFlow ~> merge
      partition.out(1) ~> Flow[Option[In]].collect { case None ⇒ None } ~> noneFlow ~> merge

      FlowShape(partition.in, merge.out)
    })
}

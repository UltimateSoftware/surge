// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.akka.streams.graph

import akka.NotUsed
import akka.stream.scaladsl.{ Flow, GraphDSL, Merge, Partition }
import akka.stream.{ FlowShape, Graph }

object OptionFlow {
  def apply[In, Out](someFlow: Flow[In, Out, NotUsed], noneFlow: Flow[None.type, Out, NotUsed]): Graph[FlowShape[Option[In], Out], NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      def partitioner(o: Option[In]): Int = o.map(_ => 0).getOrElse(1)
      val partition = builder.add(Partition[Option[In]](2, partitioner))
      val merge = builder.add(Merge[Out](2))

      partition.out(0) ~> Flow[Option[In]].collect { case Some(t) => t } ~> someFlow ~> merge
      partition.out(1) ~> Flow[Option[In]].collect { case None => None } ~> noneFlow ~> merge

      FlowShape(partition.in, merge.out)
    })
}

object EitherFlow {
  def apply[Left, Right, Out](leftFlow: Flow[Left, Out, NotUsed], rightFlow: Flow[Right, Out, NotUsed]): Graph[FlowShape[Either[Left, Right], Out], NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      def partitioner(o: Either[Left, Right]): Int = o.left.toOption.map(_ => 0).getOrElse(1)
      val partition = builder.add(Partition[Either[Left, Right]](2, partitioner))
      val merge = builder.add(Merge[Out](2))

      partition.out(0) ~> Flow[Either[Left, Right]].collect { case Left(t) => t } ~> leftFlow ~> merge
      partition.out(1) ~> Flow[Either[Left, Right]].collect { case Right(t) => t } ~> rightFlow ~> merge

      FlowShape(partition.in, merge.out)
    })
}

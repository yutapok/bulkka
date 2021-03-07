package bulkka.api

import akka.NotUsed

import akka.stream.scaladsl._

import scala.concurrent.Future


trait BulkkaTemplate[I, S, F, O] {
  def toTaked(in: I): Source[S, NotUsed]
  def toTakedFuture(inF: Future[I]): Source[S, NotUsed]
  def toTransformed: Flow[S, F, NotUsed]
  def toFolded: Sink[F, Future[O]]
}


class BulkkaEngine {
  def runGraph[I, S, F, O, T <: BulkkaTemplate[I, S, F, O]](bka: T, inOpt: Option[I], inFOpt: Option[Future[I]]): Future[O] = {
    val flow = bka.toTransformed
    val sink = bka.toFolded
    val graph = inOpt match {
      case Some(in) => bka.toTaked(in).via(flow).toMat(sink)(Keep.right)
      case None => inFOpt match {
        case Some(inF) => {
          bka.toTakedFuture(inF).via(flow).toMat(sink)(Keep.right)
        }
        case None => throw new RuntimeException("Error: Source input must be set at least 1")
      }
    }
    graph.run()
  }

}

object BulkkaEngine extends BulkkaEngine

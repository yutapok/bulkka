package akka.stream.bulkka

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.stream.{Materializer,Attributes}
import akka.{NotUsed, Done}
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.Try

import java.io._
import java.util.zip._


class Gz {
  def DecompressionFlow: Flow[Iterator[ByteString], Iterator[String], NotUsed] =
    Flow.fromMaterializer { (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[Iterator[ByteString]]
        .map(seq =>
          seq.map(bStr =>
            decompress(bStr.toArray).getOrElse("")
          ).toIterator
        )
    }
      .mapMaterializedValue(_ => NotUsed)

  def decompress(data: Array[Byte]): Option[String] = {
    (for {
      bais <- Try(new ByteArrayInputStream(data))
      gzis <- Try(new GZIPInputStream(bais))
    } yield scala.io.Source.fromInputStream(gzis).mkString)
      .toOption
  }
}

object Gz extends Gz

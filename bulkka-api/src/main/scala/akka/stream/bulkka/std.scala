package akka.stream.bulkka

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl._
import akka.http.scaladsl.model._
import akka.NotUsed
import akka.util.ByteString

import akka.stream.{Materializer,Attributes, IOResult}
import akka.stream.scaladsl._

import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.s3.ObjectMetadata

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._


class Std {
  def stdoutSink: Sink[ByteString, Future[IOResult]] = {
    val completion = Promise[IOResult]
    Sink.fromMaterializer{ (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      StreamConverters.fromOutputStream(() => System.out)

    }
      .mapMaterializedValue(_ => completion.future)
  }
}


object Std extends Std

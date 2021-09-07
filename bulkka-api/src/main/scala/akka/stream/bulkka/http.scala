package akka.stream.bulkka

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl._
import akka.http.scaladsl.model._
import akka.NotUsed
import akka.util.ByteString

import akka.stream.{Materializer,Attributes}
import akka.stream.scaladsl._

import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.s3.ObjectMetadata

import scala.concurrent.Future
import scala.concurrent.duration._


case class BHttpResponse(httpResponse: HttpResponse, responseBody: ByteString){
    lazy val headers = httpResponse.getHeaders()
    lazy val status = httpResponse.status
    lazy val entity = httpResponse.entity
    lazy val bodyRaw = responseBody
    lazy val body = responseBody.utf8String
}


class BHttp {
  lazy val PROCESSORS = Runtime.getRuntime.availableProcessors
  def rawRequestFlow: Flow[HttpRequest, HttpResponse, NotUsed] = {
    Flow.fromMaterializer{ (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[HttpRequest]
        .mapAsyncUnordered[HttpResponse](PROCESSORS)(hr => Http().singleRequest(hr))
    }
      .mapMaterializedValue(_ => NotUsed)
  }

  def requestFlow: Flow[HttpRequest, BHttpResponse, NotUsed] = {
    Flow.fromMaterializer{ (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[HttpRequest]
        .via(rawRequestFlow)
        .via(responseFlow)
    }
      .mapMaterializedValue(_ => NotUsed)
  }

  def responseFlow: Flow[HttpResponse, BHttpResponse, NotUsed] = {
    Flow.fromMaterializer{ (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[HttpResponse]
        .mapAsyncUnordered[BHttpResponse](PROCESSORS)(hres =>
          (for {
            s <- hres.entity.dataBytes.runFold(ByteString(""))(_++_)
          } yield new BHttpResponse(hres, s))
        )

    }
      .mapMaterializedValue(_ => NotUsed)
  }


}

object BHttp extends BHttp

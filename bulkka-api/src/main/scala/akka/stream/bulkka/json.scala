package akka.stream.bulkka

import akka.actor.ActorSystem
import akka.{NotUsed, Done}

import akka.stream.Materializer
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.Try

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Json4sScalaModule

import scala.reflect.ClassTag

import jp.jdf._

class Json {
    def jdfFlow[T](statements: Array[Statement], t: Class[T]): Flow[Iterator[String], Iterator[T], NotUsed] =
      Flow.fromMaterializer{ (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        implicit val materializer: Materializer = mat
        import mat.executionContext

        Flow[Iterator[String]]
          .map(itr =>
            itr.map(str =>
              Jdf.query(str, statements).getOrElse("{}")
            )
          )
          .via(toFlow(t)(mat))
      }
        .mapMaterializedValue(_ => NotUsed)

    def toFlow[T](x: Class[T])(implicit mat: Materializer): Flow[Iterator[String], Iterator[T], NotUsed] = {
      Flow.fromMaterializer{ (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        implicit val materializer: Materializer = mat
        import mat.executionContext

        Flow[Iterator[String]]
          .map(itr =>
            itr.map(iStr => mapper.readValue(iStr, x)
          ))
      }
        .mapMaterializedValue(_ => NotUsed)

    }

}


object Json extends Json

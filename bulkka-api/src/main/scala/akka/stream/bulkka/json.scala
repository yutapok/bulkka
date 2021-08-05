package akka.stream.bulkka

import akka.NotUsed
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
    def jdfFlow[T](statements: Array[Statement], t: T): Flow[Iterator[String], Iterator[T], NotUsed] =
      Flow.fromMaterializer{ (mat, attr) =>
        Flow[Iterator[String]]
          .map(itr =>
            itr.map(str =>
              Jdf.query(str, statements).getOrElse("{}")
          ))
          .via(toFlow(t))
      }
        .mapMaterializedValue(_ => NotUsed)

    def toFlow[T](x: T): Flow[Iterator[String], Iterator[T], NotUsed] =
      Flow[Iterator[String]]
        .map(itr =>
          itr.map(iStr => mapper.readValue(iStr, x.getClass)
        ))

}


object Json extends Json

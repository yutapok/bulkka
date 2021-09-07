package akka.stream.bulkka

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.NotUsed
import akka.stream.{Materializer,Attributes}
import akka.stream.scaladsl._
import akka.util.ByteString

import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.s3.ObjectMetadata

import scala.concurrent.Future
import scala.concurrent.duration._

import java.nio.file._
import java.nio.file.Paths

object ImplFile {
  def pathTmpStr(s: String, ext: String) =  s"""/tmp/${s}.${ext}"""
}

case class FileKey(name: String, ext: String){
  def toTmpPath = Paths.get(ImplFile.pathTmpStr(name, ext))
  def toName = s"""${name}.${ext}"""
  def read: ByteString = {
    val linesStr = scala.io.Source
      .fromFile(toTmpPath.toString)
      .getLines
      .mkString("\n")

    ByteString(linesStr)
  }
}

trait BaseFile {
    def write(mode: StandardOpenOption): Unit
    def key: FileKey
}

class File {
  def toTextFlow: Flow[Iterator[String], Iterator[String], NotUsed] = {
    Flow.fromMaterializer{ (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[Iterator[String]]
        .map(itr => itr.flatMap(_.split("\n").toIterator))
    }
      .mapMaterializedValue(_ => NotUsed)
  }

  def writelnDistributedFlow[T <: BaseFile]: Flow[T, FileKey, NotUsed] = {
    Flow.fromMaterializer{ (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[T]
        .map(bf => (bf.key, bf.write(StandardOpenOption.APPEND)))
        .via(writelnCompletedAtWindowFlow)
    }
      .mapMaterializedValue(_ => NotUsed)

  }

  def writelnCompletedAtWindowFlow: Flow[(FileKey, Unit), FileKey, NotUsed] = {
    Flow.fromMaterializer{ (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[(FileKey, Unit)]
        .map{case(fk, _) => fk}
        .groupedWithin(100, 1.seconds)
        .mapConcat(items =>
          items
            .sortBy(fk => (Option(fk.name), Option(fk.ext)))
            .distinct
        )

    }
    .mapMaterializedValue(_ => NotUsed)
  }

  def fromFileKeyFlow[T <: FileKey]: Flow[T, (String, ByteString), NotUsed] = {
    Flow.fromMaterializer { (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[T]
        .map(fkey => (fkey.toName, fkey.read))
    }
      .mapMaterializedValue(_ => NotUsed)
  }

}

object File extends File

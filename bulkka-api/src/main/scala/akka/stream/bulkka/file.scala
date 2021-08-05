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

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.nio.file.{Files, Paths}


object ImplFile {
  def pathTmpStr(s: String, ext: String) =  s"""/tmp/${s}.${ext}"""
}

case class FileKey(name: String, ext: String){
  def toTmpPath = Paths.get(ImplFile.pathTmpStr(name, ext))
  def toName = s"""${name}.${ext}"""
}

trait BaseFile {
    def write: Unit
    def read: ByteString
    def key: FileKey
}

case class CsvHeeader(columns: Seq[String]){
    def toCsvString: String = columns.mkString(",")
}

case class CsvBody(rows: Iterator[Seq[String]]) {
    def toCsvString: String = rows.map(seq => seq.mkString(",")).mkString("\n")
}

case class Csv(header: CsvHeeader, body: CsvBody, keyname: String) extends BaseFile {
    override def write: Unit = {
      val tmpPath = ImplFile.pathTmpStr(keyname, "csv")
      val path = Paths.get(tmpPath)
      val fileExist = Files.notExists(path)

      val writer = new PrintWriter(new BufferedWriter(new FileWriter(path.toFile, true)))
      if (fileExist){
        writer.write(header.toCsvString)
      }

      writer.write(body.toCsvString)

      writer.close

    }

    override def read: ByteString = {
        val linesStr = scala.io.Source
          .fromFile(key.toTmpPath.toString)
          .getLines
          .mkString("\n")

        ByteString(linesStr)
    }


    override def key: FileKey = new FileKey(keyname, "csv")
}

class File {
  def writelnDistributedFlow[T <: BaseFile](t: T): Flow[T, FileKey, NotUsed] = {
    Flow.fromMaterializer{ (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[T]
        .map(bf => (bf.key, bf.write))
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
          items.sortBy(fk => (fk.name, fk.ext)).distinct
        )

    }
      .mapMaterializedValue(_ => NotUsed)
  }
}

object File extends File

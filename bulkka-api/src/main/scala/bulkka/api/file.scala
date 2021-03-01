package bulkka.api

import akka.NotUsed
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.Try

import java.io._
import java.util.zip._

case class InFile(content: String){
  def toRows: Seq[String] = content.split("\n").map(row => row.replace("\r", ""))
}

class Decompress extends BulkkaTemplate[Seq[Array[Byte]], Array[Byte], InFile, Seq[InFile]]
  with MixinImplBase {
    override def toTaked(in: Seq[Array[Byte]]): Source[Array[Byte], NotUsed] =
      Source.fromIterator(() => in.toIterator)

    override def toTakedFuture(inF: Future[Seq[Array[Byte]]]): Source[Array[Byte], NotUsed] = {
      val srcF = Source.future(inF)
      srcF.flatMapConcat(seq => Source.fromIterator(() => seq.toIterator))
    }

    override def toTransformed: Flow[Array[Byte], InFile, NotUsed] =
      Flow[Array[Byte]].map(
        bArr => new InFile(implInF.decompressGz(bArr).getOrElse(""))
      )

    override def toFolded: Sink[InFile, Future[Seq[InFile]]] =
      Sink.seq
}

class InFileRows extends BulkkaTemplate[Seq[InFile], InFile, Seq[String], Seq[String]]
  with MixinImplBase {
    override def toTaked(in: Seq[InFile]): Source[InFile, NotUsed] =
      Source.fromIterator(() => in.toIterator)

    override def toTakedFuture(inF: Future[Seq[InFile]]): Source[InFile, NotUsed] = {
      val srcF = Source.future(inF)
      srcF.flatMapConcat(seq => Source.fromIterator(() => seq.toIterator))
    }

    override def toTransformed: Flow[InFile, Seq[String], NotUsed] =
      Flow[InFile].map(
        inFile => implInF.convertRows(inFile)
      )

    override def toFolded: Sink[Seq[String], Future[Seq[String]]] =
      Sink.fold(Seq.empty[String])((acc, seq) => acc ++ seq)
}

class ImplInFile {
  def convertRows(fileData: InFile): Seq[String] = fileData.toRows
  def decompressGz(data: Array[Byte]): Option[String] = {
    (for {
      bais <- Try(new ByteArrayInputStream(data))
      gzis <- Try(new GZIPInputStream(bais))
    } yield scala.io.Source.fromInputStream(gzis).mkString)
      .toOption
  }
}

class BulkkaEngineFile {
  lazy val engine: BulkkaEngine = BulkkaEngine
  def runGraphGzDecompress(inOpt: Option[Seq[Array[Byte]]], inFOpt: Option[Future[Seq[Array[Byte]]]]): Future[Seq[InFile]] = {
    engine.runGraph[
      Seq[Array[Byte]],
      Array[Byte],
      InFile,
      Seq[InFile],
      Decompress
    ](Decompress, inOpt, inFOpt)
  }

  def runGraphInFileRows(inOpt: Option[Seq[InFile]], inFOpt: Option[Future[Seq[InFile]]]): Future[Seq[String]] = {
    engine.runGraph[
      Seq[InFile],
      InFile,
      Seq[String],
      Seq[String],
      InFileRows
    ](InFileRows, inOpt, inFOpt)
  }
}


object Decompress extends Decompress
object InFileRows extends InFileRows
object ImplInFile extends ImplInFile
object BulkkaEngineFile extends BulkkaEngineFile

trait UseImplDecompress {
    val implInF: ImplInFile
}

trait MixinImplBase extends UseImplDecompress {
    override val implInF: ImplInFile = ImplInFile
}

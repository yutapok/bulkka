package bulkka.api

import akka.{NotUsed, Done}
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.Try

import java.io._
import java.util.zip._


sealed abstract class FILE_TYPE(val str:String){
  def toStr: String = str
}
case object FT_JSON extends FILE_TYPE("json")
case object FT_CSV extends FILE_TYPE("csv")
case object FT_PLAIN extends FILE_TYPE("txt")

sealed abstract class ENCODE(val str:String){
  def toStr: String = str
}
case object ENC_UTF8 extends ENCODE("UTF-8")

sealed abstract class TERMINATION(val str: String){
  def toStr = str
}
case object LF extends TERMINATION("\n")
case object CRLF extends TERMINATION("\r\n")

case class InFile(content: String){
  def toRows: Seq[String] = content.split("\n").map(row => row.replace("\r", ""))
}

case class OutFile[T](
  name: String,
  path: String,
  ftype: FILE_TYPE,
  contents: Seq[T],
  encode: ENCODE,
  termination: TERMINATION,
  append: Boolean
){
  def toFileStr = contents.mkString(termination.toStr) + termination.toStr
  def toFilePath = new File(s"/${path}/${name}.${ftype.toStr}")
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

class OutFileRows extends BulkkaTemplate[Seq[OutFile[String]], OutFile[String], (OutputStreamWriter, String), Done]
  with MixinImplBase {
    override def toTaked(in: Seq[OutFile[String]]): Source[OutFile[String], NotUsed] =
      Source.fromIterator(() => in.toIterator)

    override def toTakedFuture(inF: Future[Seq[OutFile[String]]]): Source[OutFile[String], NotUsed] = {
      val srcF = Source.future(inF)
      srcF.flatMapConcat(seq => Source.fromIterator(() => seq.toIterator))
    }

    override def toTransformed: Flow[OutFile[String], (OutputStreamWriter, String), NotUsed] =
      Flow[OutFile[String]].map(of => {
        val fileOutPutStream = new FileOutputStream(of.toFilePath, of.append)
        val writer = new OutputStreamWriter(fileOutPutStream, of.encode.toStr)
        (writer, of.toFileStr)
      })

    override def toFolded: Sink[(OutputStreamWriter, String), Future[Done]] =
      Sink.foreach{case((w, s)) => implOutF.writeStr(w, s)}
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

class ImplOutFile {
  def writeStr(writer: OutputStreamWriter, rowStr: String){
    writer.write(rowStr)
    writer.close()
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

  def runGraphOutFileRows(inOpt: Option[Seq[OutFile[String]]], inFOpt: Option[Future[Seq[OutFile[String]]]]): Future[Done] = {
    engine.runGraph[
      Seq[OutFile[String]],
      OutFile[String],
      (OutputStreamWriter, String),
      Done,
      OutFileRows
    ](OutFileRows, inOpt, inFOpt)
  }
}


object Decompress extends Decompress
object InFileRows extends InFileRows
object OutFileRows extends OutFileRows
object ImplInFile extends ImplInFile
object ImplOutFile extends ImplOutFile
object BulkkaEngineFile extends BulkkaEngineFile

trait UseImplBase {
    val implInF: ImplInFile
    val implOutF: ImplOutFile
}

trait MixinImplBase extends UseImplBase{
    override val implInF: ImplInFile = ImplInFile
    override val implOutF: ImplOutFile = ImplOutFile
}

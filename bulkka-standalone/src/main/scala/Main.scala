import scala.util.{Try, Success, Failure}
import org.ini4j._

import java.io._

import bulkka.api._

object Main extends App
  with MixinImpl {

  val iniPath = args(0)
  val paramsT = Try(impl.parseIni(iniPath))

  paramsT match {
    case Success(params) => run(params)
    case Failure(msg) => println(msg)
  }



  private def run(params: Map[String, Map[String, String]]){
    val srcType = params("Source")("type")
    val bct = params("Source")("s3.bucket")
    val key = params("Source")("s3.key")

    val s3RawsF = bc.s3.runGraphGets(Some((bct, key)), None)
      .map(seq => seq.flatMap(s3Raw => s3Raw.contents)
        .map(bs => bs.toArray)
      )

    val decompF = bc.file.runGraphGzDecompress(None, Some(s3RawsF))
      .map(seq => seq.flatMap(inFi => inFi.toRows))

    val jsonedF = bc.json.runGraphJsonParseX(None, Some(decompF))

    val fields = params("Transform.json")
      .filter{case((k,v)) => k.startsWith("fields")}


    val jsonKeyPairs = fields.map{case((k, v)) =>
      (k.split("\\.")(1), v)
    }

    val mapF = (for {
      jsonedSeq <- jsonedF
    } yield for {
      jsoned <- jsonedSeq
    } yield jsonKeyPairs.map{case((l, n)) => (l, jsoned.get(n))})

    val aggedKeyValueF = (for {
      mapSeq <- mapF
    } yield for {
      mp <- mapSeq
      (label, valueOpt) <- mp
      valueAny <- valueOpt
    } yield (label, impl.aggAny(label, valueAny)))

    val fname = params("Sink")("filename")
    val path = params("Sink")("path")

    val oFF = aggedKeyValueF.map(seq =>
      Seq(new OutFile(
        fname,
        path,
        FT_PLAIN,
        seq.map{case((t1, t2)) => s"${t1},${t2}"},
        ENC_UTF8,
        LF,
        true
      )
    ))

    val retF = bc.file.runGraphOutFileRows(None, Some(oFF))

    retF.onComplete(_ => system.terminate())
    coodiShutdown
  }
}


class Impl {
  def parseIni(filePath: String): Map[String, Map[String, String]] = {
    val ini = new Ini(new File(filePath))
    val blocks = ini.keySet.toArray.map(_.toString)
    (for {
      block <- blocks
      keys = ini.get(block).keySet.toArray.map(_.toString)
      values = ini.get(block).values.toArray.map(_.toString)
    } yield (block, keys.zip(values).toMap)).toMap
  }

  //TODO: make module
  def aggAny(label: String, vAny: Any): Any = {
    label match {
      case "impression" => vAny.asInstanceOf[Seq[BigInt]].sum
      case _ => vAny
    }
  }

}

object Impl extends Impl

trait UseImpl {
  val impl: Impl
}

trait MixinImpl extends UseImpl {
  override val impl: Impl = Impl
}

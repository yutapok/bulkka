package bulkka.api

import akka.NotUsed
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.Try

import org.json4s._
import org.json4s.jackson.JsonMethods._


class Json extends BulkkaTemplate[Seq[String], String, Map[String, Any], Seq[Map[String, Any]]]
  with MixinImplJsonBase {
    override def toTaked(in: Seq[String]): Source[String, NotUsed] =
      Source.fromIterator(() => in.toIterator)

    override def toTakedFuture(inF: Future[Seq[String]]): Source[String, NotUsed] = {
      val srcF = Source.future(inF)
      srcF.flatMapConcat(seq => Source.fromIterator(() => seq.toIterator))
    }

    override def toTransformed: Flow[String, Map[String, Any], NotUsed] =
      Flow[String].map(
        str => (for {
          jobj <- Try(parse(str).asInstanceOf[JObject]).toOption
        } yield impl.parseRecursive(jobj.values))
          .getOrElse(Map.empty[String, Any])
      )

    override def toFolded: Sink[Map[String, Any], Future[Seq[Map[String, Any]]]] =
      Sink.seq
}


class ImplJson {
  def parseRecursive(mp: Map[String, Any]): Map[String, Any] = {
    val anyTo = (for {
      (k, v) <- mp
    } yield {
      val sqAnyT = Try(v.asInstanceOf[Seq[Any]])
      val mpT = Try(v.asInstanceOf[Map[String, Any]])
      if (sqAnyT.isSuccess) {
        if (sqAnyT.get == null) Map(k -> null)
        else Map(k + ".[]" -> sqAnyT.get.map(
          any => if (Try(any.asInstanceOf[Map[String, Any]]).isSuccess) parseRecursive(Map(k + ".[]" ->  any))
                 else any
        ))

      } else if (mpT.isSuccess){
        val mp_ = mpT.get.asInstanceOf[Map[String, Any]]
        parseRecursive(mp_.map{case((key_, v)) => k + "." + key_ -> v})

      } else {
        val jobT = Try(parse(v.asInstanceOf[String]).asInstanceOf[JObject])
        if (jobT.isSuccess) parseRecursive(jobT.get.values.map{case((k_,v)) => (k + "." + k_, v)})
        else Map(k -> v.asInstanceOf[Any])

      }
    })

    anyTo.foldLeft(Map[String, Any]())((acc, mp) => acc ++ mp)

  }
}

class BulkkaEngineJson {
  lazy val engine: BulkkaEngine = BulkkaEngine
  def runGraphJsonParseX(inOpt: Option[Seq[String]], inFOpt: Option[Future[Seq[String]]]): Future[Seq[Map[String, Any]]] = {
    engine.runGraph[
      Seq[String],
      String,
      Map[String, Any],
      Seq[Map[String, Any]],
      Json
    ](Json, inOpt, inFOpt)
  }
}


object Json extends Json
object ImplJson extends ImplJson
object BulkkaEngineJson extends BulkkaEngineJson

trait UseImplJson {
    val impl: ImplJson
}

trait MixinImplJsonBase extends UseImplJson {
    override val impl: ImplJson = ImplJson
}

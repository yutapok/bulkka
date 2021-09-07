package akka.stream.bulkka

import akka.actor.ActorSystem
import akka.{NotUsed, Done}

import akka.stream.Materializer
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.Try
import scala.util.matching.Regex

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Json4sScalaModule

import java.io._
import java.io.RandomAccessFile

import java.nio._
import java.nio.charset.StandardCharsets
import java.nio.file._

import collection.JavaConverters._


case class Table(rows: TableRows, keyname: String, tt: TableType) extends BaseFile {
    override def write(mode: StandardOpenOption): Unit = {
      val tmpJoinPath = if (tt == TableType.New) {
        ImplFile.pathTmpStr(keyname, "temp.join")

      } else {
        ImplFile.pathTmpStr(keyname, "temp.join.unpair")

      }

      val path = Paths.get(tmpJoinPath)
      if (!path.toFile.exists) {
        path.toFile.createNewFile()
      }

      Files.write(path, rows.toArrayBytes, mode)
      ()
    }

    override def key: FileKey = if (tt == TableType.New) {
      new FileKey(keyname, "temp.join")

    } else {
      new FileKey(keyname, "temp.join.unpair")

    }
}


case class TableRows(itr: Iterator[TableRow]){
    def toArrayBytes: Array[Byte] = {
        val s = itr
          .map(tr => tr.toRawString)
          .mkString("\n")

        s"${s}\n"
          .getBytes(StandardCharsets.UTF_8.name())
    }

    def merge(trows: TableRows): TableRows = {
        new TableRows(itr ++ trows.itr)
    }
}

case class TableRow(ix: Int, key: String, source: Option[String], dest: Option[String]){
    def toRawString = s"""${key}${source.getOrElse("")}${dest.getOrElse("")}"""
}

object ImplJoin extends ImplJoin {}

sealed trait TableType
object TableType {
  case object New extends TableType
  case object Unpair extends TableType
}

sealed trait RowType
object RowType {
  case object Source extends RowType
  case object Dest extends RowType
}

sealed trait Relation
object Relation {
  case object OneToOne extends Relation
  case object OneToMany extends Relation
}

case class Joined(keyname: String, srcSolved: Array[Int], dstSolved: Array[Int], mode: Relation) {
  def aggregate(srcItr: IndexedSeq[(String, Map[String,Any])], dstItr: IndexedSeq[(String, Map[String,Any])]) = {
    val (srcPairs, srcUnPairs) = srcSolved
      .zipWithIndex
      .partition{case((v, ix)) => v == 1}

    val (_, dstUnPairs) = dstSolved
      .zipWithIndex
      .partition{case((v, ix)) => v == 1}


    if (mode == Relation.OneToMany) {
      Table(
        stock(srcItr, srcPairs.map{case((v,i)) => i}, RowType.Source),
        keyname,
        TableType.New
      ).write(
        StandardOpenOption.APPEND
      )
    }

    val sRows = stock(srcItr, srcUnPairs.map{case((v,i)) => i}, RowType.Source)
    val dRows = stock(dstItr, dstUnPairs.map{case((v,i)) => i}, RowType.Dest)

    Table(
      sRows.merge(dRows),
      keyname,
      TableType.Unpair
    ).write(
      StandardOpenOption.WRITE
    )

  }

  def stock(itr: IndexedSeq[(String, Map[String,Any])], indexes: Array[Int], rt: RowType): TableRows = {
    val tblRows = (for {
     (key, mp)  <- indexes.map(ix => itr(ix))
    } yield TableRow.apply(
      -1,
      key,
      if (rt == RowType.Source) Some(mapper.writeValueAsString(mp.asJava)) else None,
      if (rt == RowType.Source) None else Some(mapper.writeValueAsString(mp.asJava)),
    ))

    new TableRows(tblRows.toIterator)
  }
}

class ImplJoin {
    lazy val patternJoinTableRow: Regex = "^(?<key>[^]*)(?<src>[^]*)(?<dst>[^]*)$".r

    def load(keyname: String, load: TableType): Option[Iterator[TableRow]] = {
      val tmpJoinPath = if (load == TableType.New) {
          ImplFile.pathTmpStr(keyname, "temp.join.lock")
      } else {
          ImplFile.pathTmpStr(keyname, "temp.join.unpair")
      }

      val pathJoin = Paths.get(tmpJoinPath)
      if (!pathJoin.toFile.exists) {
         None

      } else {
        Some(loadPath(pathJoin, load))

      }
    }

    private def loadPath(p: Path, load: TableType): Iterator[TableRow] = {
      val lines = Files.readAllLines(p)
        .toArray
        .map(_.toString)

      val tableRowsOpt  = (for {
        (line, ix) <- lines.zipWithIndex
      } yield line match {
        case patternJoinTableRow(s1, s2, s3) => {
          val src: Option[String] = if (s2.isEmpty) None else Some(s2)
          val dst: Option[String] = if (s3.isEmpty) None else Some(s3)
          Some(TableRow.apply(
            if (load == TableType.New) ix else -1,
            s1,
            src,
            dst
          ))
        }
        case _ => None
      })

      tableRowsOpt
        .filter(_.nonEmpty)
        .map(_.get)
        .toIterator

    }

    def lock(keyname: String): Unit = {
      val tmpJoinPath = ImplFile.pathTmpStr(keyname, "temp.join")
      val lockTmpJoinPath = ImplFile.pathTmpStr(keyname, "temp.join.lock")
      val path = Paths.get(tmpJoinPath)
      val newPath = Paths.get(lockTmpJoinPath)

      if (path.toFile.exists && !newPath.toFile.exists) {
        Files.move(path, newPath)

      }
    }


    def joinByKey(keyname: String, mode: Relation): Iterator[(String, Map[String,Any], Map[String,Any])] = {
       lock(keyname)

       val (srcItr, dstItr) = (for {
         tr <- load(keyname, TableType.New).getOrElse(Iterator.empty)
         if (tr.source.nonEmpty || tr.dest.nonEmpty)
       } yield tr)
        .partition(tr => tr.source.nonEmpty)

      val srcMapItr = (for {
        tr <- srcItr
        src <- tr.source
      } yield (tr.key, parse(src).asInstanceOf[JObject].values))
        .toIndexedSeq

      val destMapItr = (for {
        tr <- dstItr
        dst <- tr.dest
      } yield (tr.key, parse(dst).asInstanceOf[JObject].values))
        .toIndexedSeq

      val srcSolved = new Array[Int](srcMapItr.length)
      val dstSolved = new Array[Int](destMapItr.length)

      var results = Seq.empty[(String, Map[String,Any], Map[String,Any])]
      for (((srcKey, srcV), i) <- srcMapItr.zipWithIndex) {
        for (j <- 0 to destMapItr.length - 1) {
          if (dstSolved(j) != 1) {
            val (dstKey, dstV) = destMapItr(j)
            if (srcKey == dstKey && srcV.get(srcKey).getOrElse("- ") == dstV.get(dstKey).getOrElse(" -")) {
              srcSolved(i) = 1
              dstSolved(j) = 1
              results = results :+  (srcKey, srcV, dstV.asInstanceOf[Map[String, Any]])
            }
          }
        }
      }

      val joined = new Joined(keyname, srcSolved, dstSolved, mode)

      joined.aggregate(srcMapItr, destMapItr)

      results.toIterator

    }
}

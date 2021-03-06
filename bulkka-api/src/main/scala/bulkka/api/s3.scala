package bulkka.api

import akka.NotUsed
import akka.stream.scaladsl._
import akka.util.ByteString

import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.s3.ObjectMetadata

import scala.concurrent.Future


case class S3Key(bucket: String, content: String)
case class S3FileRawData(contents: Seq[ByteString])

class S3Gets extends BulkkaTemplate[(String, String), S3Key, S3FileRawData, Seq[S3FileRawData]]
  with MixinImplBulkkaS3 {
    override def toTaked(in: (String, String)): Source[S3Key, NotUsed] = {
      val (bucket, bucketKey) = in
      val keysF = impl.keys(bucket, bucketKey)
      val srcF = Source.future(keysF)
      srcF.flatMapConcat(seq => Source.fromIterator(() => seq.toIterator))
    }

    override def toTakedFuture(inF: Future[(String, String)]): Source[S3Key, NotUsed] = {
      val keysF = (for {
        (bucket, bucketKey) <- inF
      } yield impl.keys(bucket, bucketKey))
        .flatMap(f => f)

      val srcF = Source.future(keysF)
      srcF.flatMapConcat(seq => Source.fromIterator(() => seq.toIterator))
    }

    override def toTransformed: Flow[S3Key, S3FileRawData, NotUsed] =
      Flow[S3Key].mapAsyncUnordered[S3FileRawData](impl.PROCESSORS)(
        s3key => impl.download(s3key)
      )

    override def toFolded: Sink[S3FileRawData, Future[Seq[S3FileRawData]]] =
      Sink.seq
}


class ImplBulkkaS3 {
  lazy val PROCESSORS = Runtime.getRuntime.availableProcessors
  def keys(bucket: String, bucketKey: String): Future[Seq[S3Key]] = {
    val listBucketF = S3.listBucket(bucket, Some(bucketKey)).runWith(Sink.seq)
    val keysF: Future[Seq[S3Key]] = (for {
      listBucket <- listBucketF
    } yield listBucket
      .filter(lbrc => lbrc.size != 0)
      .map(lbrc => new S3Key(bucket, lbrc.key)))

    keysF
  }

  def download(s3key: S3Key): Future[S3FileRawData] = {
    val s3DlOptSeqF = S3.download(s3key.bucket, s3key.content).runWith(Sink.head)

    val s3FRData = (for {
      s3DlOpt: Option[(Source[ByteString,NotUsed], ObjectMetadata)] <- s3DlOptSeqF
    } yield for {
      s3Dl <- s3DlOpt
      (data, _) = s3Dl
    } yield data.runWith(Sink.seq[ByteString]))

    s3FRData.flatMap{
      case Some(vF) => vF
      case None => Future.successful(Seq[ByteString]())
    }.map{
      seq => new S3FileRawData(seq)
    }
  }
}

class BulkkaEngineS3 {
  lazy val engine: BulkkaEngine = BulkkaEngine
  def runGraphGets(inOpt: Option[(String, String)], inFOpt: Option[Future[(String, String)]]): Future[Seq[S3FileRawData]] = {
    engine.runGraph[
      (String, String),
      S3Key,
      S3FileRawData,
      Seq[S3FileRawData],
      S3Gets
    ](S3Gets, inOpt, inFOpt)
  }
}

object S3Gets extends S3Gets
object BulkkaEngineS3 extends BulkkaEngineS3
object ImplBulkkaS3 extends ImplBulkkaS3

trait UseImplBulkkaS3 {
  val impl: ImplBulkkaS3
}

trait MixinImplBulkkaS3 extends UseImplBulkkaS3 {
  override val impl: ImplBulkkaS3 = ImplBulkkaS3
}



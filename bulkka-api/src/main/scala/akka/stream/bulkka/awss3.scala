package akka.stream.bulkka

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.{NotUsed, Done}
import akka.stream.{Materializer,Attributes}
import akka.stream.scaladsl._
import akka.util.ByteString

import akka.stream.alpakka.s3._
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.s3.ObjectMetadata

import scala.concurrent.Future


case class S3Key(bucket: String, content: String)
case class S3FileRawData(contents: Iterator[ByteString])

class AwsS3 {
  lazy val PROCESSORS = Runtime.getRuntime.availableProcessors
  def bulkConcatDownload(bucket: String, bucketKey: String): Source[Iterator[ByteString], NotUsed] = {

    Source.fromMaterializer { (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      val keysF = keys(bucket, bucketKey)(mat, attr)
      Source.future(keysF)
        .flatMapConcat(seq => Source.fromIterator(() => seq.toIterator))
        .via(fetchRawDataFlow)
        .via(toByteStringFlow)
    }
      .mapMaterializedValue(_ => NotUsed)
  }

  def toByteStringFlow: Flow[S3FileRawData, Iterator[ByteString], NotUsed] =
    Flow.fromMaterializer { (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[S3FileRawData]
        .map(data => data.contents)
    }
      .mapMaterializedValue(_ => NotUsed)

  def bulkMultiUpload(bucket: String): Sink[(String, ByteString), Future[Done]] = {
    Sink.fromMaterializer { (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[(String, ByteString)]
        .map{ case((keyStr, contents)) => (S3Key(bucket, keyStr), contents) }
        .map{ case((s3key, contents)) => upload(s3key, contents)(mat, attr) }
        .to(Sink.ignore)
    }
      .mapMaterializedValue(_ => Future.successful(Done))
  }

  def keyFromFileFlow[T <: BaseFile]: Flow[T, (String, ByteString), NotUsed] = {
    Flow.fromMaterializer { (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[T]
        .map(file => (file.key.toName, file.read))
    }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def fetchRawDataFlow: Flow[S3Key, S3FileRawData, NotUsed] =
    Flow.fromMaterializer { (mat, attr) =>
      implicit val system: ActorSystem = mat.system
      implicit val materializer: Materializer = mat
      import mat.executionContext

      Flow[S3Key].mapAsyncUnordered[S3FileRawData](PROCESSORS)(
        s3key => download(s3key)(mat, attr)
      )
    }
      .mapMaterializedValue(_ => NotUsed)

  private def keys(bucket: String, bucketKey: String)(implicit mat: Materializer, attr: Attributes): Future[Seq[S3Key]] = {
    import mat.executionContext
    val listBucketF = S3.listBucket(bucket, Some(bucketKey)).withAttributes(attr).runWith(Sink.seq)
    val keysF: Future[Seq[S3Key]] = (for {
      listBucket <- listBucketF
    } yield listBucket
      .filter(lbrc => lbrc.size != 0)
      .map(lbrc => new S3Key(bucket, lbrc.key)))

    keysF
  }

  private def download(s3key: S3Key)(implicit mat: Materializer, attr: Attributes): Future[S3FileRawData] = {
    import mat.executionContext
    val s3DlOptSeqF = S3.download(s3key.bucket, s3key.content).withAttributes(attr).runWith(Sink.head)

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
      seq => new S3FileRawData(seq.toIterator)
    }
  }

  private def upload(s3key: S3Key, bString: ByteString)(implicit mat: Materializer, attr: Attributes): Future[MultipartUploadResult] = {
    import mat.executionContext
    Source
      .single(bString)
      .withAttributes(attr)
      .runWith(S3.multipartUpload(s3key.bucket, s3key.content))
  }
}

object AwsS3 extends AwsS3

package bulkka

import akka.actor.ActorSystem
import akka.stream.scaladsl._

import akka.stream.ActorMaterializer

import scala.concurrent._

import bulkka.api._

package object api{
  implicit lazy val materializer = ActorMaterializer()
  implicit lazy val system: ActorSystem = ActorSystem("bulkka-api")
  implicit lazy val ec: ExecutionContextExecutor = system.dispatcher
  implicit lazy val PROCESSORS = Runtime.getRuntime.availableProcessors
  implicit lazy val bc = BulkkaContext
}

object BulkkaContext extends MixInBulkkaEngine

trait UseBulkkaEngine {
  val s3: BulkkaEngineS3
  val file: BulkkaEngineFile
  val json: BulkkaEngineJson
}

trait MixInBulkkaEngine extends UseBulkkaEngine {
  override val s3: BulkkaEngineS3 = BulkkaEngineS3
  override val file: BulkkaEngineFile = BulkkaEngineFile
  override val json: BulkkaEngineJson = BulkkaEngineJson
}

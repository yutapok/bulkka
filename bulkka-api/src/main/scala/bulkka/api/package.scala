package bulkka

import akka.actor.ActorSystem
import akka.actor.CoordinatedShutdown

import akka.stream.KillSwitches
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
  implicit lazy val sharedKillSwitch = KillSwitches.shared("bulkka-core-kill-switch")
  implicit def coodiShutdown = {
    CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "bulkka-core-shutdown") {
      () => sharedKillSwitch.shutdown()
      Future.successful(akka.Done)
    }
  }

}

object BulkkaContext extends MixInBulkkaEngine

trait UseBulkkaEngine {
  val core: BulkkaEngine
  val s3: BulkkaEngineS3
  val file: BulkkaEngineFile
  val json: BulkkaEngineJson
}

trait MixInBulkkaEngine extends UseBulkkaEngine {
  override val core: BulkkaEngine = BulkkaEngine
  override val s3: BulkkaEngineS3 = BulkkaEngineS3
  override val file: BulkkaEngineFile = BulkkaEngineFile
  override val json: BulkkaEngineJson = BulkkaEngineJson
}

package gridgaintest

import org.gridgain.scalar.scalar
import scalar._
import org.gridgain.grid._
import collection.mutable.HashSet
import java.util.UUID
import GridGainUtil._
import logger.GridLogger
import resources.{GridLoggerResource, GridTaskSessionResource}

trait StreamingMonteCarloSimulation {
  type ModelData
  type LocalStatistics
  type Aggregator <: GridTaskLinkedStatisticsAggregatorActor[LocalStatistics]
  type Result

  def createGridJob(master: GridRichNode, workerId: Int): MonteCarloSimulationGridJob[ModelData, LocalStatistics]

  def createAggregator(future: GridOneWayTaskFuture): Aggregator

  /**
   * Triggers the GridJob to start the simulation by writing the ModelData into the GridTaskSession.
   */
  final def startSimulation(taskSession: GridTaskSession): Unit = {
    taskSession.setAttribute(StreamingMonteCarloSimulation.ModelDataAttributeKey, modelData)
  }

  def modelData: ModelData

  def extractResult(aggregator: Aggregator): Result

  def apply(maxWorkers: Int, grid: Grid): Result = {
    val workerIds = (1 to maxWorkers).toList
    val master = grid.localNode

    // Create a GridTask that will create a GridJob for each workerId.
    val task: GridOneWayTask[List[Int]] = GridGainUtil.splitOnlyGridTask[List[Int]](_.map(id => createGridJob(master, id)))

    // Execute this task on remote nodes. The GridJob will block, awaiting the model to be writing the the GridTaskSession.
    val future: GridOneWayTaskFuture = grid.remoteProjection().execute(task, workerIds)

    // Register Statistics Aggregator Actor on the master node.
    val aggregator = createAggregator(future)
    grid.listenAsync(aggregator)

    // Communicate through the GridTaskSession to trigger the GridJobs to start calculation.
    startSimulation(future.getTaskSession)

    // Wait for either: a) completion of all GridJobs, or b) cancellation of the task by the StatisticsAggregator.
    waitForCompletionOrCancellation(future)

    extractResult(aggregator)
  }
}

object StreamingMonteCarloSimulation {
  val ModelDataAttributeKey: String = "modelData"
}

trait GridTaskLinkedStatisticsAggregatorActor[T] extends StatisticsAggregatorActor[T] {
  import GridGainUtil._

  val future: GridOneWayTaskFuture

  def cancel() = {
    println("GridTaskLinkedStatisticsAggregatorActor.cancel")
    future.cancel
  }

  val taskId = future.getTaskSession.getId

}


abstract class MonteCarloSimulationGridJob[Model, T](master: GridRichNode, workerId: Int) extends CancellableGridJob with GridTaskSessionAware with GridLoggerAware {
  val MaxSimulationBatchesPerWorker: Int = 10000

  @GridTaskSessionResource
  def setTaskSession(taskSes: GridTaskSession) = this.taskSes = taskSes

  @GridLoggerResource
  def setLogger(logger: GridLogger) = this.logger = logger


  override def logInfo(msg: => String) = {
    assert(taskSes != null)
    super.logInfo("taskID: %s, workerID: %d || %s".format(taskSes.getId, workerId, msg))
  }

  // This is required to force distributed class loading of the implementation class for the trait, before the job is cancelled.
  // TODO boil down a smaller example and report problem to GridGain.
  forceLoad

  def execute(): AnyRef = {
    logInfo("execute(), waiting for modelData attribute")
    val modelData: Model = taskSes.waitForAttribute(StreamingMonteCarloSimulation.ModelDataAttributeKey)
    logInfo("got model data, starting simulation")
    for (batchId <- 1 to MaxSimulationBatchesPerWorker) {
      val localStatistics = simulationBatch(batchId, modelData)
      val message = LocalStatisticsMessage(taskSes.getId, (workerId, batchId), localStatistics)
      if (cancelled) {
        return null
      }

      try {
        logDebug("sending results from: %s".format(message))
        master !< message
      } catch {
        case e: GridRuntimeException =>
          logInfo("warning (cancellation may be in progress):" + e.getMessage)
          return null
      }
    }
    null
  }

  def simulationBatch(batchId: Int, modelData: Model): T
}

case class LocalStatisticsMessage[T](taskID: UUID, statsID: Any, stats: T)

/**
 * An actor that listens for message of type `T`. If the message is related to the current
 * task, and has not been previously processed, it is handed to `process`.
 */
abstract class StatisticsAggregatorActor[T] extends GridListenActor[LocalStatisticsMessage[T]] {
  val processed = new HashSet[Any]()

  private def processOnce(localStats: LocalStatisticsMessage[T])(f: T => Unit) = {
    val alreadyProcessed = processed.contains(localStats.statsID)
    if (!alreadyProcessed) {
      processed += localStats.statsID
      f(localStats.stats)
    }
  }

  sealed abstract class StoppingDecision
  case object Stop extends StoppingDecision
  case object Continue extends StoppingDecision

  def receive(nodeId: UUID, localStats: LocalStatisticsMessage[T]) {
    if (localStats.taskID == taskId) {
      processOnce(localStats) {
        stats: T =>
          process(stats) match {
            case Continue =>
            case Stop =>
              cancel
              stop() // It this is a trait: java.lang.IllegalAccessError: tried to access method org.gridgain.grid.GridListenActor.stop()V from class gridgaintest.StatisticsAggregatorActor$class
          }
      }
    }
  }

  val taskId: UUID

  def process(stats: T): StoppingDecision

  def cancel(): Unit
}

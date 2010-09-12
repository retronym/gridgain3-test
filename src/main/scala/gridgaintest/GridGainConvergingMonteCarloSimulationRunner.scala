package gridgaintest

import org.gridgain.grid._
import java.util.UUID
import GridGainUtil._
import resources.GridTaskSessionResource
import java.util.concurrent.TimeUnit
import java.lang.String

class GridGainConvergingMonteCarloSimulationRunner(maxWorkers: Int, maxSimulations: Int, simulationsPerBlock: Int, grid: Grid) extends ConvergingMonteCarloSimulationRunner {
  def apply[R](sim: ConvergingMonteCarloSimulation[R]): ConvergingMonteCarloSimulationResult[R] = {
    import sim._

    val workerIds = (1 to maxWorkers).toList
    val master = grid.localNode

    val start = System.nanoTime

    val (initGlobalStats, modelData) = initialize

    // Create a GridTask that will create a GridJob for each workerId.
    val maxSimulationsPerWorker: Int = (maxSimulations.toDouble / maxWorkers.toDouble).ceil.toInt
    val task: GridOneWayTask[List[Int]] = GridGainUtil.splitOnlyGridTask[List[Int]](_.map {
      id =>
        new SimGridJob[LocalStatistics](id, createWorker(id, simulationsPerBlock), maxSimulationsPerWorker, master)
    })

    // Execute this task on remote nodes. The GridJob will block, awaiting the model to be writing the the GridTaskSession.
    val taskFuture: GridOneWayTaskFuture = grid.execute(task, workerIds)

    // Register Statistics Aggregator Actor on the master node.
    val aggregator = new GridTaskLinkedStatisticsAggregatorActor[LocalStatistics] {
      var globalStats: GlobalStatistics = initGlobalStats

      val future = taskFuture

      var lastSimControl: SimulationControl = _

      def process(localStats: LocalStatistics): SimulationControl = {
        val (simulationControl, newGlobalStats) = aggregate(globalStats, localStats)
        lastSimControl = simulationControl
        globalStats = newGlobalStats
        simulationControl
      }

      def result = lastSimControl match {
        case Stop => Completed(extractResult(globalStats))
        case _ => ConvergenceFailed
      }
    }

    grid.listenAsync(aggregator)

    // Communicate through the GridTaskSession to trigger the GridJobs to start calculation.
    aggregator.setSessionAttribute(GridGainConvergingMonteCarloSimulationRunner.ModelDataAttributeKey, modelData)

    val modelDataSent = System.nanoTime

    // Wait for either: a) completion of all GridJobs, or b) cancellation of the task by the StatisticsAggregator.
    waitForCompletionOrCancellation(taskFuture)

    val elapsed = {
      val end = System.nanoTime
      TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val simMetrics = SimMetrics(elapsed, aggregator.jobMetrics.map(_.elapsedMs))
    println(simMetrics)

    aggregator.result
  }
}

sealed abstract class SimMessage {
  val taskID: UUID
}
case class LocalStatisticsMessage[T](taskID: UUID, statsID: Any, stats: T) extends SimMessage
case class MetricsMessage(taskID: UUID, workerID: Int, elapsedMs: Long) extends SimMessage

case class SimMetrics(elapsedMs: Long, workerMetrics: Seq[Long])

object GridGainConvergingMonteCarloSimulationRunner {
  val ModelDataAttributeKey = "modelData"
  val BroadcastAttributeKey = "broadcast"
}

class SimGridJob[LocalStatistics](workerId: Int, worker: (Int, Option[Any]) => LocalStatistics, maxSimulationsPerJob: Int, master: GridRichNode) extends GridJob {
  @volatile var cancelled = false

  var taskSes: GridTaskSession = _

  @GridTaskSessionResource
  def setTaskSession(taskSes: GridTaskSession) = this.taskSes = taskSes

  def execute: AnyRef = {
    val start = System.nanoTime
    try {
      execute0
    } finally {
      val end = System.nanoTime
      val elapsedMs: Long = TimeUnit.NANOSECONDS.toMillis(end - start)
      val metrics: MetricsMessage = MetricsMessage(taskSes.getId, workerId, elapsedMs)
      master.send(metrics)
    }
  }

  private def execute0: AnyRef = {
    val model: Pi.ModelData = taskSes.waitForAttribute(GridGainConvergingMonteCarloSimulationRunner.ModelDataAttributeKey)

    def toOption[T >: Null](t: T) = if (t == null) None else Some(t)

    def execute(blockId: Int): Unit = {
      val broadcast = toOption(taskSes.getAttribute(GridGainConvergingMonteCarloSimulationRunner.BroadcastAttributeKey))
      if (broadcast == Some(Stop)) cancel

      if (!cancelled && blockId < maxSimulationsPerJob) {
        val localStatistics = worker(blockId, broadcast)
        val msg = LocalStatisticsMessage(taskSes.getId, (workerId, blockId), localStatistics)
        try {
          master.send(msg)
        } catch {
          case x: GridRuntimeException =>
        }
        execute(blockId + 1)
      }
    }
    execute(0)
    null
  }

  def cancel = cancelled = true
}

/**
 * An actor that listens for message of type `T`. If the message is related to the current
 * task, and has not been previously processed, it is handed to `process`.
 */
abstract class StatisticsAggregatorActor[T] extends GridListenActor[SimMessage] {
  private val processOnce = new OneTime[LocalStatisticsMessage[T], T](_.statsID, _.stats)
  val jobMetrics = new collection.mutable.ListBuffer[MetricsMessage]()

  protected val taskId: UUID

  /**
   * Update the internal state of the aggregator with the local statistics, and decide
   * whether to continue or stop the simulation.
   */
  protected def process(localStatistics: T): SimulationControl

  protected def broadcast(msg: Any): Unit

  def receive(nodeId: UUID, simMsg: SimMessage) {
    if (simMsg.taskID == taskId) {
      simMsg match {
        case m: MetricsMessage =>
          jobMetrics += m
        case msg: LocalStatisticsMessage[_] =>
          processOnce(msg.asInstanceOf[LocalStatisticsMessage[T]]) {
            stats: T =>
              process(stats) match {
                case Continue =>
                case BroadcastAndContinue(msg) =>
                  broadcast(msg)
                case Stop =>
                  broadcast(Stop)
              }
          }
      }
    }
  }
}

trait GridTaskLinkedStatisticsAggregatorActor[T] extends StatisticsAggregatorActor[T] {
  import GridGainUtil._

  val future: GridOneWayTaskFuture

  def setSessionAttribute(key: String, value: Any) = future.getTaskSession.setAttribute(key, value.asInstanceOf[AnyRef])

  def broadcast(msg: Any) = setSessionAttribute(GridGainConvergingMonteCarloSimulationRunner.BroadcastAttributeKey, msg)

  val taskId = future.getTaskSession.getId
}



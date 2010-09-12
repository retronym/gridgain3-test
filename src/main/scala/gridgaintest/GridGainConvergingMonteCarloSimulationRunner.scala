package gridgaintest

import org.gridgain.grid._
import GridGainUtil._
import java.util.concurrent.TimeUnit


class GridGainConvergingMonteCarloSimulationRunner(maxWorkers: Int, maxSimulations: Int, simulationsPerBlock: Int, grid: Grid) extends ConvergingMonteCarloSimulationRunner {
  def apply[R](sim: ConvergingMonteCarloSimulation[R]): ConvergingMonteCarloSimulationResult[R] = {
    import sim._

    val workerIds = (1 to maxWorkers).toList
    val master = grid.localNode

    val start = System.nanoTime

    val (initGlobalStats, modelData) = initialize

    // Create a GridTask that will create a GridJob for each workerId.
    val maxSimulationBlocksPerWorker: Int = ((maxSimulations.toDouble / maxWorkers.toDouble).ceil / simulationsPerBlock.toDouble).ceil.toInt
    val task: GridOneWayTask[List[Int]] = GridGainUtil.splitOnlyGridTask[List[Int]](_.map {
      id =>
        new SimGridJob[LocalStatistics](id, createWorker(id, simulationsPerBlock), maxSimulationBlocksPerWorker, master)
    })

    // Execute this task on remote nodes. The GridJob will block, awaiting the model to be writing the the GridTaskSession.
    val taskFuture: GridOneWayTaskFuture = grid.execute(task, workerIds)

    // Register Statistics Aggregator Actor on the master node.
    val aggregatorActor = new StatisticsAggregatorActor[LocalStatistics] {
      var globalStats: GlobalStatistics = initGlobalStats

      val future = taskFuture

      var lastSimControl: SimulationControl = _

      var blocksProcessed = 0

      def process(localStats: LocalStatistics): SimulationControl = {
        blocksProcessed += 1
        val (simulationControl, newGlobalStats) = aggregator(globalStats, localStats)
        lastSimControl = simulationControl
        globalStats = newGlobalStats
        simulationControl
      }

      def result = lastSimControl match {
        case Stop => Completed(blocksProcessed * simulationsPerBlock, extractResult(globalStats))
        case _ => ConvergenceFailed
      }
    }

    grid.listenAsync(aggregatorActor)

    // Communicate through the GridTaskSession to trigger the GridJobs to start calculation.
    aggregatorActor.setSessionAttribute(GridGainConvergingMonteCarloSimulationRunner.ModelDataAttributeKey, modelData)

    val modelDataSent = System.nanoTime

    // Wait for either: a) completion of all GridJobs, or b) cancellation of the task by the StatisticsAggregator.
    waitForCompletionOrCancellation(taskFuture)

    val elapsed = {
      val end = System.nanoTime
      TimeUnit.NANOSECONDS.toMillis(end - start)
    }
    val simMetrics = SimMetrics(elapsed, aggregatorActor.jobMetrics.map(_.elapsedMs))
    println(simMetrics)

    aggregatorActor.result
  }
}

object GridGainConvergingMonteCarloSimulationRunner {
  val ModelDataAttributeKey = "modelData"
  val BroadcastAttributeKey = "broadcast"
}



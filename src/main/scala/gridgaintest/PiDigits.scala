package gridgaintest

import org.gridgain.scalar.scalar
import scalar._
import org.gridgain.grid._
import java.security.SecureRandom
import actors.Futures
import Futures._
import GridGainUtil._


object PiDigits {
  val MaxWorkers: Int = 16

  case class ModelData(dummy: Array[Byte])
  val modelData = ModelData(new Array[Byte](8 * 1024 * 1024))

  def main(args: Array[String]) {
    scalar {
      (grid: Grid) =>
        // Run a few simulations in parallel to check thread safety of the StatisticsAggregatorActor.
        val pis = (1 to 2).map(_ => future(Simulation(MaxWorkers, grid))).map(_.apply)
        println(pis)
    }
  }

  /**
   * Monte Carlo simulation to approximate Pi. The problem is divided between GridJobs, who process
   * blocks of simulations. The results of each block are streamed back to the Aggregator on the master
   * node. The aggregator can stop the simulation once the variance is low enough.
   */
  object Simulation extends StreamingMonteCarloSimulation {
    type ModelData = PiDigits.ModelData
    type LocalStatistics = Array[Boolean]
    type Aggregator = StatisticsAggregator
    type Result = Double

    def modelData = PiDigits.modelData

    def createGridJob(master: GridRichNode, workerId: Int) = new GridJob(master, workerId)

    def createAggregator(future: GridOneWayTaskFuture) = new StatisticsAggregator(future)

    def extractResult(aggregator: Aggregator): Result = aggregator.result
  }

  class GridJob(master: GridRichNode, workerId: Int) extends MonteCarloSimulationGridJob[ModelData, Array[Boolean]](master, workerId) {
    val SimulationsPerBlock: Int = 1000

    val r = new SecureRandom()

    def simulationBatch(batchId: Int, modelData: ModelData): Array[Boolean] = {
      val ics = for (j <- 1 to SimulationsPerBlock) yield {
        val (x, y) = (r.nextDouble, r.nextDouble)
        val inCircle: Boolean = (x * x + y * y) <= 1d
        inCircle
      }
      ics.toArray
    }
  }

  class StatisticsAggregator(val future: GridOneWayTaskFuture) extends GridTaskLinkedStatisticsAggregatorActor[Array[Boolean]] {
    val MinSamples: Int = 128
    val RequiredVariance: Double = 0.001

    var batches = 0
    var total = 0
    var inCircle = 0
    val varianceStat = new MeanVarianceOnlineStatistic

    def process(localStats: Array[Boolean]): StoppingDecision = {
      for (ic <- localStats) {
        total += 1
        if (ic) inCircle += 1
      }
      batches += 1
      varianceStat(4 * inCircle.toDouble / total.toDouble)
      if (batches > MinSamples && varianceStat.variance < RequiredVariance) Stop else Continue
    }

    def result = varianceStat.mean
  }
}

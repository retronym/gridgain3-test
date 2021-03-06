package gridgaintest.example
import gridgaintest._

import java.security.SecureRandom
import gridgaintest.util.MeanVarianceOnlineStatistic

/**
 * Monte Carlo simulation to approximate Pi. The problem is divided between GridJobs, who process
 * blocks of simulations. The results of each block are streamed back to the Aggregator on the master
 * node. The aggregator can stop the simulation once the variance is low enough.
 */
object Pi {
  case class ModelData(dummy: Array[Byte])

  class Simulation(requiredVariance: Double) extends ConvergingMonteCarloSimulation[Double] {
    type LocalStatistics = Array[Boolean]
    type GlobalStatistics = MeanVarianceOnlineStatistic
    type ModelData = Pi.ModelData

    def initialize = (new GlobalStatistics, ModelData(new Array[Byte](8 * 1024 * 1024)))

    def createWorker(workerId: Int, simulationsPerBlock: Int) = new PiWorker(workerId, simulationsPerBlock)

    val aggregator: Aggregator = new PiAggregator(requiredVariance)

    def extractResult(global: GlobalStatistics): Double = global.mean
  }

  class PiAggregator(requiredVariance: Double) extends ((MeanVarianceOnlineStatistic, Array[Boolean]) => (SimControlMessage, MeanVarianceOnlineStatistic)) {
    var total = 0
    var inCircle = 0

    def apply(global: MeanVarianceOnlineStatistic, localStats: Array[Boolean]) = {
      total += localStats.length
      for (ic <- localStats if ic) inCircle += 1
      global(4 * inCircle.toDouble / total.toDouble)
      val decision = if (total > 128 && global.variance < requiredVariance) Stop
      else if (total % 10000 == 0) BroadcastAndContinue("n = %d, μ = %f, σ² = %f".format(total, global.mean, global.variance))
      else Continue

      (decision, global)
    }
  }

  class PiWorker(workerID: Int, simulationsPerBlock: Int) extends ((Int, Option[Any]) => Array[Boolean]) {
    def this() = this (-1, -1) // Default constructor needed for serialization.

    val r = new SecureRandom()

    def apply(blockID: Int, broadcast: Option[Any]) = {
      println("(workerID, blockID): (%d, %d), broadcast: %s".format(workerID, blockID, broadcast.toString))
      val ics = for (j <- 1 to simulationsPerBlock) yield {
        val (x, y) = (r.nextDouble, r.nextDouble)
        val inCircle: Boolean = (x * x + y * y) <= 1d
        inCircle
      }
      ics.toArray
    }
  }

}
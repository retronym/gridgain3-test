package gridgaintest

import java.security.SecureRandom

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

    def createWorker(workerId: Int, simulationsPerBlock: Int) = new Worker(simulationsPerBlock)

    val aggregate: Aggregator = new PiAggregator(requiredVariance)

    def extractResult(global: GlobalStatistics): Option[Double] = Some(global.mean)
  }

  class PiAggregator(requiredVariance: Double) extends ((MeanVarianceOnlineStatistic, Array[Boolean]) => (SimulationControl, MeanVarianceOnlineStatistic)) {
    var total = 0
    var inCircle = 0

    def apply(global: MeanVarianceOnlineStatistic, localStats: Array[Boolean]) = {
      total += localStats.length
      for (ic <- localStats if ic) inCircle += 1
      global(4 * inCircle.toDouble / total.toDouble)
      val decision = if (total > 128 && global.variance < requiredVariance) Stop
        else if (total % 1000 == 0) BroadcastAndContinue("keep up the good work!")
      else Continue
      (decision, global)
    }
  }

  class Worker(simulationsPerBlock: Int) extends ((Int, Option[Any]) => Array[Boolean]) {

    def this() = this (-1) // Default constructor needed for serialization.

    val r = new SecureRandom()

    def apply(blockID: Int, broadcast: Option[Any]) = {
      println("blockID: %d, %s".format(blockID, broadcast.toString))
      val ics = for (j <- 1 to simulationsPerBlock) yield {
        val (x, y) = (r.nextDouble, r.nextDouble)
        val inCircle: Boolean = (x * x + y * y) <= 1d
        inCircle
      }
      ics.toArray
    }
  }

}

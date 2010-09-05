package gridgaintest

import org.gridgain.scalar.scalar
import scalar._
import org.gridgain.grid._
import java.security.SecureRandom
import java.util.UUID
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
        val fs = List(future[Double] {simulatePi(grid)}, future[Double] {simulatePi(grid)})
        println(fs.map(_.apply))
    }
  }

  def simulatePi(implicit grid: Grid): Double = Simulation(MaxWorkers, grid)

  object Simulation extends MonteCarloSimulation {
    type LocalStats = Array[Boolean]
    type Aggregator = StatisticsAggregator
    type Result = Double
    type ModelData = PiDigits.ModelData

    def createJob(master: GridRichNode, workerId: Int) = new GridJob(master, workerId)

    def extractResult(aggregator: Aggregator): Result = aggregator.result

    def modelData = PiDigits.modelData

    def createAggregator(future: GridGainUtil.GridOneWayTaskFuture) = new StatisticsAggregator(future)
  }

  class StatisticsAggregator(val future: GridGainUtil.GridOneWayTaskFuture) extends GridTaskLinkedStatisticsAggregatorActor[Array[Boolean]] {
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
      println("processed: %s, pi=%f variance=%f".format(localStats, varianceStat.mean, varianceStat.variance))
      if (batches > MinSamples && varianceStat.variance < RequiredVariance) Stop else Continue
    }

    def result = varianceStat.mean
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
}

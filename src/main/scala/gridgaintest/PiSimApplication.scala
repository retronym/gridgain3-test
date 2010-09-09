package gridgaintest

import org.gridgain.scalar.scalar
import actors.Futures._
import actors.Future
import java.util.concurrent.TimeUnit
import org.gridgain.grid.resources.GridTaskSessionResource
import org.gridgain.grid._
import java.util.UUID

object PiSimApplication {
  def main(args: Array[String]) {
    val MaxSimulations = 100000
    val SimsPerBlock = 1000
    val RequiredVariance = 0.0001
    val NumWorkers = 8
    val GridRestarts: Int = 100
    val ConcurrentSimulations = 1

    for (i <- 0 until GridRestarts) scalar {
      (grid: Grid) =>
        val runners = Seq(
          new SimpleConvergingMonteCarloSimulationRunner(MaxSimulations, SimsPerBlock),
          new GridGainConvergingMonteCarloSimulationRunner(NumWorkers, MaxSimulations, SimsPerBlock, grid))

        val sims: Seq[Future[(String, Option[Double])]] = for{
          runner <- runners // Use different simulation runners
          sim <- 0 until ConcurrentSimulations // Run a few simulations in parallel to check thread safety of the StatisticsAggregatorActor
        } yield future((runner.getClass.getSimpleName, runner(new Pi.Simulation(RequiredVariance))))

        // await the futures
        val results: Seq[(String, Option[Double])] = sims.map(_())

        println(results.mkString("\n"))
    }
  }
}

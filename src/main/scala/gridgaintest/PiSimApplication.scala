package gridgaintest

import org.gridgain.grid.Grid
import org.gridgain.scalar.scalar
import actors.Futures._
import actors.Future

object PiSimApplication {
  def main(args: Array[String]) {
    val MaxSimulations = 100000
    val SimsPerBlock = 1000
    val RequiredVariance = 0.0001
    val NumWorkers = 8

    for (i <- 1 to 100) scalar {
      (grid: Grid) =>
        val runners = Seq(
          new SimpleConvergingMonteCarloSimulationRunner(MaxSimulations, SimsPerBlock),
          new GridGainConvergingMonteCarloSimulationRunner(NumWorkers, MaxSimulations, SimsPerBlock, grid))

        val sims: Seq[Future[(String, Option[Double])]]= for{
          runner <- runners // Use different simulation runners
          sim <- 1 to 8     // Run a few simulations in parallel to check thread safety of the StatisticsAggregatorActor
        } yield future((runner.getClass.getSimpleName, runner(new Pi.Simulation(RequiredVariance))))

        // await the futures
        val results: Seq[(String, Option[Double])] = sims.map(_())

        println(results.mkString("\n"))
    }
  }
}

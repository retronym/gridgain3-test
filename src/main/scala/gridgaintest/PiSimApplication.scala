package gridgaintest

import org.gridgain.grid.Grid
import org.gridgain.scalar.scalar
import scala.actors.Futures._

object PiSimApplication {
  def main(args: Array[String]) {
    val MaxSimulations = 100000
    val SimsPerBlock = 1000
    val RequiredVariance: Double = 0.0001
    val NumWorkers: Int = 8

    scalar {
      (grid: Grid) =>
        val runners = Seq(
          new SimpleConvergingMonteCarloSimulationRunner(MaxSimulations, SimsPerBlock),
          new GridGainConvergingMonteCarloSimulationRunner(NumWorkers, MaxSimulations, SimsPerBlock, grid))
        val sims = for{
          runner <- runners // Use different simulation runners
          sim <- 1 to 2     // Run a few simulations in parallel to check thread safety of the StatisticsAggregatorActor
        } yield {
          future((runner.getClass.getSimpleName,
                  runner(new Pi.Simulation(RequiredVariance))))
        }
        println(sims.map(_()).mkString("\n"))
    }
  }
}
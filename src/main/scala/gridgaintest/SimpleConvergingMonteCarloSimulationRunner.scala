package gridgaintest

class SimpleConvergingMonteCarloSimulationRunner(maxSimulations: Int, simulationsPerBlock: Int) extends ConvergingMonteCarloSimulationRunner {
  def apply[R](sim: ConvergingMonteCarloSimulation[R]): Option[R] = {
    import sim._
    val (initGlobalStats, model) = initialize
    val worker = createWorker(0, simulationsPerBlock)
    val maxBlocks = (maxSimulations.toDouble / simulationsPerBlock.toDouble).ceil.toInt

    def simulate(blockID: Int, globalStats: sim.GlobalStatistics): Option[GlobalStatistics] = {
      val (decision, newStats) = aggregate(globalStats, worker())
      if (decision == Stop)
        Some(globalStats)
      else if (blockID > maxBlocks)
        None
      else
        simulate(blockID + 1, newStats)
    }
    simulate(0, initGlobalStats) flatMap extractResult
  }
}

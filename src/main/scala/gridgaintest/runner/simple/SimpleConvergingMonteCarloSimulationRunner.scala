package gridgaintest.runner.simple

class SimpleConvergingMonteCarloSimulationRunner(maxSimulations: Int, simulationsPerBlock: Int) extends ConvergingMonteCarloSimulationRunner {
  def apply[R](sim: ConvergingMonteCarloSimulation[R]): ConvergingMonteCarloSimulationResult[R] = {
    import sim._
    val (initGlobalStats, model) = initialize
    val worker = createWorker(0, simulationsPerBlock)
    val maxBlocks = (maxSimulations.toDouble / simulationsPerBlock.toDouble).ceil.toInt
    var broadcast: Option[Any] = None
    def simulate(blockID: Int, globalStats: sim.GlobalStatistics): ConvergingMonteCarloSimulationResult[R] = {
      val (simulationControl, newStats) = aggregator(globalStats, worker(blockID, broadcast))
      if (blockID > maxBlocks)
        ConvergenceFailed
      else {
        simulationControl match {
          case Stop =>
            Completed(blockID * simulationsPerBlock, extractResult(newStats))
          case Continue =>
            simulate(blockID + 1, newStats)
          case BroadcastAndContinue(msg) =>
            broadcast = Some(msg)
            simulate(blockID + 1, newStats)
        }
      }
    }
    simulate(0, initGlobalStats)
  }
}
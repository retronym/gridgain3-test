package gridgaintest

sealed abstract class StoppingDecision
case object Stop extends StoppingDecision
case object Continue extends StoppingDecision

trait ConvergingMonteCarloSimulation[Result] {
  type ModelData
  type LocalStatistics
  type GlobalStatistics

  def initialize: (GlobalStatistics, ModelData)

  def createWorker(workerId: Int, model: ModelData): () => LocalStatistics

  def aggregate(global: GlobalStatistics, local: LocalStatistics): (StoppingDecision, GlobalStatistics)

  def extractResult(aggregator: GlobalStatistics): Option[Result]
}

trait ConvergingMonteCarloSimulationRunner {
  def run[R](sim: ConvergingMonteCarloSimulation[R]): Option[R]
}

class SimpleConvergingMonteCarloSimulationRunner extends ConvergingMonteCarloSimulationRunner {
  def run[R](sim: ConvergingMonteCarloSimulation[R]): Option[R] = {
    import sim._
    val (initGlobalStats, model) = initialize
    val worker = createWorker(0, model)
    def simulate(blockID: Int, globalStats: sim.GlobalStatistics): Option[GlobalStatistics] = {
      val (decision, newStats) = aggregate(globalStats, worker())
      if (decision == Stop)
        Some(globalStats)
      else if (blockID > 100000)
        None
      else
        simulate(blockID + 1, newStats)
    }
    simulate(0, initGlobalStats) flatMap extractResult
  }
}

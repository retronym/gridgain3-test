package gridgaintest

sealed abstract class StoppingDecision
case object Stop extends StoppingDecision
case object Continue extends StoppingDecision

trait ConvergingMonteCarloSimulation[Result] {
  type ModelData
  type LocalStatistics
  type GlobalStatistics

  def initialize: (GlobalStatistics, ModelData)

  def createWorker(workerId: Int, simulationsPerBlock: Int): () => LocalStatistics

  type Aggregator = (GlobalStatistics, LocalStatistics) => (StoppingDecision, GlobalStatistics)

  val aggregate: Aggregator

  def extractResult(aggregator: GlobalStatistics): Option[Result]
}

trait ConvergingMonteCarloSimulationRunner {
  def apply[R](sim: ConvergingMonteCarloSimulation[R]): Option[R]
}

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

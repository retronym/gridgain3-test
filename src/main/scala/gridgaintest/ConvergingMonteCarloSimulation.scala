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

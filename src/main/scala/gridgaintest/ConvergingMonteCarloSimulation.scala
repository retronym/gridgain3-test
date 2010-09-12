package gridgaintest

trait ConvergingMonteCarloSimulation[Result] {
  type ModelData

  /**
   * Statistics gathered locally by a worker for a single block of simulations.
   */
  type LocalStatistics

  /**
   * Statistics aggregated on the master, based on the stream of LocalStatistics
   */
  type GlobalStatistics

  /**
   * A worker is a function that accepts the ID of the simulation block and the latest
   * broadcast message, and yields LocalStatistics.
   */
  type Worker = (Int, Option[Any]) => LocalStatistics

  /**
   * An aggregator is a function that update the GlobalStatistics with a LocalStatistics.
   * If the convergence criteria is met, the simulation can be stopped.
   */
  type Aggregator = (GlobalStatistics, LocalStatistics) => (SimControlMessage, GlobalStatistics)

  def initialize: (GlobalStatistics, ModelData)

  def createWorker(workerId: Int, simulationsPerBlock: Int): Worker

  val aggregator: Aggregator

  def extractResult(aggregator: GlobalStatistics): Result
}

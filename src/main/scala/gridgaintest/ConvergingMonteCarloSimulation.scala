package gridgaintest

sealed abstract class SimulationControl
case object Stop extends SimulationControl
case object Continue extends SimulationControl
case class BroadcastAndContinue[T](t: T) extends SimulationControl

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
  type Aggregator = (GlobalStatistics, LocalStatistics) => (SimulationControl, GlobalStatistics)

  def initialize: (GlobalStatistics, ModelData)

  def createWorker(workerId: Int, simulationsPerBlock: Int): Worker

  val aggregator: Aggregator

  def extractResult(aggregator: GlobalStatistics): Result
}

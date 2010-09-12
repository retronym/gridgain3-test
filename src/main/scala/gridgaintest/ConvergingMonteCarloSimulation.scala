package gridgaintest

sealed abstract class SimulationControl
case object Stop extends SimulationControl
case object Continue extends SimulationControl
case class BroadcastAndContinue[T](t: T) extends SimulationControl

trait ConvergingMonteCarloSimulation[Result] {
  type ModelData
  type LocalStatistics
  type GlobalStatistics

  def initialize: (GlobalStatistics, ModelData)

  def createWorker(workerId: Int, simulationsPerBlock: Int): (Int, Option[Any]) => LocalStatistics

  type Aggregator = (GlobalStatistics, LocalStatistics) => (SimulationControl, GlobalStatistics)

  val aggregate: Aggregator

  def extractResult(aggregator: GlobalStatistics): Result
}

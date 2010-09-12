package gridgaintest

trait ConvergingMonteCarloSimulationRunner {
  def apply[R](sim: ConvergingMonteCarloSimulation[R]): ConvergingMonteCarloSimulationResult[R]
}

sealed abstract class ConvergingMonteCarloSimulationResult[+R]
case class Completed[+R](simulations: Int, result: R) extends ConvergingMonteCarloSimulationResult[R]
case object ConvergenceFailed extends ConvergingMonteCarloSimulationResult[Nothing]
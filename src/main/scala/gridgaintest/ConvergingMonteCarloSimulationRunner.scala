package gridgaintest

trait ConvergingMonteCarloSimulationRunner {
  def apply[R](sim: ConvergingMonteCarloSimulation[R]): Option[R]
}
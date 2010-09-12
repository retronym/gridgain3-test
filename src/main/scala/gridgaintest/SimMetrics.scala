package gridgaintest

/**
 * Metrics for the entire simulation.
 */
case class SimMetrics(elapsedMs: Long, workerMetrics: Seq[Long])
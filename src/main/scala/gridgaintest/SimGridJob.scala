package gridgaintest

import org.gridgain.grid._
import java.util.concurrent.TimeUnit
import resources.GridTaskSessionResource

class SimGridJob[LocalStatistics](workerId: Int, worker: (Int, Option[Any]) => LocalStatistics,
                                  maxSimulationBlocksPerWorker: Int, master: GridRichNode) extends GridJob {
  @volatile var cancelled = false

  var taskSes: GridTaskSession = _

  @GridTaskSessionResource
  def setTaskSession(taskSes: GridTaskSession) = this.taskSes = taskSes

  def execute: AnyRef = {
    val start = System.nanoTime
    try {
      execute0
    } finally {
      val end = System.nanoTime
      val elapsedMs: Long = TimeUnit.NANOSECONDS.toMillis(end - start)
      val metrics: MetricsMessage = MetricsMessage(taskSes.getId, workerId, elapsedMs)
      master.send(metrics)
    }
  }

  private def execute0: AnyRef = {
    val model: Pi.ModelData = taskSes.waitForAttribute(GridGainConvergingMonteCarloSimulationRunner.ModelDataAttributeKey)

    def toOption[T >: Null](t: T) = if (t == null) None else Some(t)

    def execute(blockId: Int): Unit = {
      val broadcast = toOption(taskSes.getAttribute(GridGainConvergingMonteCarloSimulationRunner.BroadcastAttributeKey))
      if (broadcast == Some(Stop)) cancel

      if (!cancelled && blockId < maxSimulationBlocksPerWorker) {
        val localStatistics = worker(blockId, broadcast)
        val msg = LocalStatisticsMessage(taskSes.getId, (workerId, blockId), localStatistics)
        try {
          master.send(msg)
        } catch {
          case x: GridRuntimeException =>
        }
        execute(blockId + 1)
      }
    }
    execute(0)
    null
  }

  def cancel = cancelled = true
}
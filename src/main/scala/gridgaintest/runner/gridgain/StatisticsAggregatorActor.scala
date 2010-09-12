package gridgaintest.runner.gridgain

import org.gridgain.grid.GridListenActor
import java.util.UUID

/**
 * An actor that listens for message of type `T`. If the message is related to the current
 * task, and has not been previously processed, it is handed to `process`.
 */
abstract class StatisticsAggregatorActor[T] extends GridListenActor[SimMessage] {
  private val processOnce = new OneTime[LocalStatisticsMessage[T], T](_.statsID, _.stats)
  val jobMetrics = new collection.mutable.ListBuffer[MetricsMessage]()

  /**
   * Update the internal state of the aggregator with the local statistics, and decide
   * whether to continue or stop the simulation.
   */
  protected def process(localStatistics: T): SimControlMessage

  def receive(nodeId: UUID, simMsg: SimMessage) {
    if (simMsg.taskID == taskId) {
      simMsg match {
        case m: MetricsMessage =>
          jobMetrics += m
        case msg: LocalStatisticsMessage[_] =>
          processOnce(msg.asInstanceOf[LocalStatisticsMessage[T]]) {
            stats: T =>
              process(stats) match {
                case Continue =>
                case BroadcastAndContinue(msg) =>
                  broadcast(msg)
                case Stop =>
                  broadcast(Stop)
              }
          }
      }
    }
  }

  import GridGainUtil._

  val future: GridOneWayTaskFuture

  def setSessionAttribute(key: String, value: Any) = future.getTaskSession.setAttribute(key, value.asInstanceOf[AnyRef])

  def broadcast(msg: Any): Unit = setSessionAttribute(GridGainConvergingMonteCarloSimulationRunner.BroadcastAttributeKey, msg)

  val taskId = future.getTaskSession.getId
}
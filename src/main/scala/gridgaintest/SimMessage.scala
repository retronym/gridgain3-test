package gridgaintest

import java.util.UUID

/**
 * Message sent from the worker to the master
 */
sealed abstract class SimMessage {
  val taskID: UUID
}
case class LocalStatisticsMessage[T](taskID: UUID, statsID: Any, stats: T) extends SimMessage
case class MetricsMessage(taskID: UUID, workerID: Int, elapsedMs: Long) extends SimMessage

package gridgaintest

import org.gridgain.grid._
import resources.{GridLoggerResource, GridTaskSessionResource}
import java.util.{Collection => JCollection, ArrayList => JArrayList}
import logger.GridLogger

object GridGainUtil {
  type GridOneWayTask[T] = GridTask[T, Void]
  type GridOneWayTaskFuture = GridTaskFuture[Void]

  /**
   * Scala friendly wrapper around GridTaskNoReduceSplitAdapter
   */
  def splitOnlyGridTask[T](splitter: T => Seq[GridJob]): GridOneWayTask[T] = new GridTaskNoReduceSplitAdapter[T] {
    def split(gridSize: Int, t: T): JCollection[_ <: GridJob] = {
      val jobs: JCollection[GridJob] = new JArrayList[GridJob]()
      for (job <- splitter(t))
        jobs.add(job)
      jobs
    }
  }

  def waitForCompletionOrCancellation(f: GridOneWayTaskFuture): Unit = try {
    f.get()
  } catch {
    case _: GridFutureCancelledException => // ignore
  }
}

trait GridTaskSessionAware {
  var taskSes: GridTaskSession = _

  // TODO Injecting not reliable through a setter in a trait. Works sometimes, not others. Why?
//  @GridTaskSessionResource
//  def setTaskSession(taskSes: GridTaskSession) = this.taskSes = taskSes
}

trait GridLoggerAware {
  var logger: GridLogger = _

//  @GridLoggerResource
//  def setLogger(logger: GridLogger) = {
//    this.logger = logger
//  }

  def logInfo(msg: => String) = {
    if (logger == null)
      println(msg)
    else if (logger.isInfoEnabled) logger.info(msg)
  }

  def logDebug(msg: => String) = if (logger.isDebugEnabled) logger.debug(msg)

  def logWarn(msg: => String) = logger.warning(msg)

  def logWarn(msg: => String, t: Throwable) = logger.warning(msg, t)

  def logError(msg: => String) = logger.error(msg)

  def logError(msg: => String, t: Throwable) = logger.error(msg, t)
}

trait CancellableGridJob extends GridJob {
  self: GridLoggerAware =>
  @volatile var cancelled: Boolean = false

  def cancel() = {
    cancelled = true
  }

  val forceLoad = ""
}


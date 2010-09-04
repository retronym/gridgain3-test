package gridgaintest

import org.gridgain.scalar.scalar
import scalar._
import org.gridgain.grid._
import java.security.SecureRandom
import java.util.concurrent.{TimeUnit, CountDownLatch}
import collection.mutable.HashSet
import java.util.{UUID, Collection => JCollection, ArrayList => JArrayList}
import actors.Futures._
import actors.Futures
import logger.GridLogger
import resources.{GridLoggerResource, GridTaskSessionResource, GridInstanceResource}
import java.lang.{String, Math}

case class ModelData(dummy: Array[Byte])
case class LocalStatistics(taskID: UUID, id: LocalStatisticsID, ics: Array[Boolean])
case class LocalStatisticsID(workerID: Int, batchID: Int)

object PiDigits {
  val MinSamples: Int = 128
  val MaxWorkers: Int = 16
  val RequiredVariance: Double = 0.00001
  val modelData = ModelData(new Array[Byte](8 * 1024 * 1024))
  val ModelDataAttributeKey: String = "modelData"

  def main(args: Array[String]) {
    scalar {
      (grid: Grid) =>
        val fs = List(future[Any] {simulatePi(grid)}, future[Any] {simulatePi(grid)})
        fs.map(_.apply)
    }
  }

  def simulatePi(implicit grid: Grid) {
    val master = grid.localNode

    val workerIds = (1 to MaxWorkers).toList

    val future: GridTaskFuture[Void] = grid.execute(new MonteCarloSimulationTask(master), workerIds)

    def info(msg: => String) = if (grid.log.isInfoEnabled) grid.log.info("taskID: %s || %s".format(future.getTaskSession.getId, msg))

    grid.listenAsync(new GridListenActor[LocalStatistics]() {
      var batches = 0
      var total = 0
      var inCircle = 0
      val varianceStat = new MeanVarianceOnlineStatistic
      val processed = new HashSet[LocalStatisticsID]()

      def receive(nodeId: UUID, localStats: LocalStatistics) {
        if (localStats.taskID != future.getTaskSession.getId) {
          info("skipping, wrong task: %s".format(localStats))
        } else if (processed.contains(localStats.id)) {
          info("ignoring duplicate: %s".format(localStats))
        } else {
          processed += localStats.id

          for (ic <- localStats.ics) {
            total += 1
            if (ic) inCircle += 1
          }
          batches += 1
          varianceStat(4 * inCircle.toDouble / total.toDouble)
          info("processed: %s, pi=%f variance=%f".format(localStats, varianceStat.mean, varianceStat.variance))
          if (batches > MinSamples && varianceStat.variance < RequiredVariance) {
            info("close enough!")
            future.cancel
            stop()
          }
        }
      }
    })


    future.getTaskSession.setAttribute(ModelDataAttributeKey, modelData)

    try {
      future.get()
      info("completed")
    } catch {
      case _: GridFutureCancelledException =>
        info("stopped early")
    }
    TimeUnit.SECONDS.sleep(5)
    ()
  }
}

class MonteCarloSimulationTask(master: GridRichNode) extends GridTaskNoReduceSplitAdapter[List[Int]] {
  def split(gridSize: Int, workerIds: List[Int]): JCollection[_ <: GridJob] = {
    val jobs: JCollection[GridJob] = new JArrayList[GridJob]()

    for ((w: Int) <- workerIds)
      jobs.add(new MonteCarloSimulationGridJob(master, w))
    jobs
  }
}

class MonteCarloSimulationGridJob(master: GridRichNode, workerId: Int) extends GridJob with Cancellable with GridTaskSessionAware with GridLoggerAware {
  val MaxSimulationBatchesPerWorker: Int = 10000

  val r = new SecureRandom()

  override def logInfo(msg: => String) = super.logInfo("taskID: %s, workerID: %d || %s".format(taskSes.getId, workerId, msg))

  def execute(): AnyRef = {
    logInfo("execute(), waiting for modelData attribute")
    val modelData: ModelData = taskSes.waitForAttribute(PiDigits.ModelDataAttributeKey)
    logInfo("got model data, starting simulation")
    for (batchId <- 1 to MaxSimulationBatchesPerWorker) {
      val localStatistics = simulationBatch(batchId)
      if (cancelled) return null
      try {
        logInfo("sending results from: %s".format(localStatistics.id))
        master !< localStatistics
      } catch {
        case e: GridRuntimeException =>
          logInfo("warning (cancellation may be in progress):" + e.getMessage)
          return null
      }
    }
    null
  }

  def simulationBatch(batchId: Int): LocalStatistics = {
    val ics = for (j <- 1 to 1000) yield {
      val (x, y) = (r.nextDouble, r.nextDouble)
      val inCircle: Boolean = (x * x + y * y) <= 1d
      inCircle
    }

    LocalStatistics(taskSes.getId, LocalStatisticsID(workerId, batchId), ics.toArray)
  }
}

class MeanVarianceOnlineStatistic {
  var mean = 0d
  var variance = 0d
  var n = 0

  private var M2 = 0d

  def apply(x: Double) = {
    n = n + 1
    val delta = x - mean
    mean = mean + delta / n.toDouble
    M2 = M2 + delta * (x - mean)

    val variance_n = M2 / n.toDouble
    variance = M2 / (n.toDouble - 1d)
    variance
  }
}

trait GridTaskSessionAware {
  var taskSes: GridTaskSession = _

  @GridTaskSessionResource
  def setTaskSession(taskSes: GridTaskSession) = this.taskSes = taskSes
}

trait GridLoggerAware {
  var logger: GridLogger = _

  @GridLoggerResource
  def setLogger(logger: GridLogger) = this.logger = logger

  def logInfo(msg: => String) = if (logger.isInfoEnabled) logger.info(msg)

  def logDebug(msg: => String) = if (logger.isDebugEnabled) logger.debug(msg)

  def logWarn(msg: => String) = logger.warning(msg)

  def logWarn(msg: => String, t: Throwable) = logger.warning(msg, t)

  def logError(msg: => String) = logger.error(msg)

  def logError(msg: => String, t: Throwable) = logger.error(msg, t)
}

trait Cancellable {
  @volatile var cancelled: Boolean = false

  def cancel() = cancelled = true
}

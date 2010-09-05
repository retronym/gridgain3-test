package gridgaintest

import org.gridgain.scalar.scalar
import scalar._
import org.gridgain.grid._
import java.security.SecureRandom
import java.util.concurrent.TimeUnit
import collection.mutable.HashSet
import java.util.{UUID, Collection => JCollection, ArrayList => JArrayList}
import actors.Futures
import Futures._
import logger.GridLogger
import resources.{GridLoggerResource, GridTaskSessionResource}
import java.lang.{String, Math}

case class ModelData(dummy: Array[Byte])
case class LocalStatistics(taskID: UUID, id: LocalStatisticsID, ics: Array[Boolean])
case class LocalStatisticsID(workerID: Int, batchID: Int)

object PiDigits {
  val MaxWorkers: Int = 16

  val modelData = ModelData(new Array[Byte](8 * 1024 * 1024))
  val ModelDataAttributeKey: String = "modelData"

  import GridGainUtil._

  def main(args: Array[String]) {
    scalar {
      (grid: Grid) =>
        val fs = List(future[Double] {simulatePi(grid)}, future[Double] {simulatePi(grid)})
        println(fs.map(_.apply))
    }
  }

  def simulatePi(implicit grid: Grid): Double = PiDigitsSimulation(MaxWorkers, grid)
}

object PiDigitsSimulation extends MonteCarloSimulation {
  type LocalStats = Array[Boolean]
  type Aggregator = PiDigitsStatisticsAggregator
  type Result = Double

  def createJob(master: GridRichNode, workerId: Int) = new PiDigitsGridJob(master, workerId)

  def extractResult(aggregator: Aggregator): Result = aggregator.result

  def afterSplit(taskSession: GridTaskSession) = taskSession.setAttribute(PiDigits.ModelDataAttributeKey, PiDigits.modelData)

  def createAggregator(future: GridGainUtil.GridOneWayTaskFuture) = new PiDigitsStatisticsAggregator(future)
}

class PiDigitsStatisticsAggregator(val future: GridGainUtil.GridOneWayTaskFuture) extends GridTaskLinkedStatisticsAggregatorActor[Array[Boolean]] {
  val MinSamples: Int = 128
  val RequiredVariance: Double = 0.001

  var batches = 0
  var total = 0
  var inCircle = 0
  val varianceStat = new MeanVarianceOnlineStatistic

  def process(localStats: Array[Boolean]): StoppingDecision = {
    for (ic <- localStats) {
      total += 1
      if (ic) inCircle += 1
    }
    batches += 1
    varianceStat(4 * inCircle.toDouble / total.toDouble)
    println("processed: %s, pi=%f variance=%f".format(localStats, varianceStat.mean, varianceStat.variance))
    if (batches > MinSamples && varianceStat.variance < RequiredVariance) Stop else Continue
  }

  def result = varianceStat.mean
}

class PiDigitsGridJob(master: GridRichNode, workerId: Int) extends MonteCarloSimulationGridJob[Array[Boolean]](master, workerId) {
  val SimulationsPerBlock: Int = 1000

  val r = new SecureRandom()

  def simulationBatch(batchId: Int): Array[Boolean] = {
    val ics = for (j <- 1 to SimulationsPerBlock) yield {
      val (x, y) = (r.nextDouble, r.nextDouble)
      val inCircle: Boolean = (x * x + y * y) <= 1d
      inCircle
    }
    ics.toArray
  }
}

trait MonteCarloSimulation {
  import GridGainUtil._

  type LocalStats
  type Aggregator <: GridTaskLinkedStatisticsAggregatorActor[LocalStats]
  type Result

  def createJob(master: GridRichNode, workerId: Int): MonteCarloSimulationGridJob[LocalStats]

  def createAggregator(future: GridOneWayTaskFuture): Aggregator

  def afterSplit(taskSession: GridTaskSession): Unit

  def extractResult(aggregator: Aggregator): Result

  def apply(maxWorkers: Int, grid: Grid): Result = {
    val workerIds = (1 to maxWorkers).toList
    val master = grid.localNode

    val task: GridOneWayTask[List[Int]] = GridGainUtil.splitOnlyGridTask[List[Int]](_.map(id => createJob(master, id)))
    grid
    val future: GridOneWayTaskFuture = grid.remoteProjection().execute(task, workerIds)
    println("local node: " + master.getId)
    println("remote nodes: " + grid.remoteNodes(null))
    println("topology: " + future.getTaskSession.getTopology)
    def info(msg: => String) = if (grid.log.isInfoEnabled) grid.log.info("taskID: %s || %s".format(future.getTaskSession.getId, msg))
    val aggregator = createAggregator(future)
    grid.listenAsync(aggregator)
    afterSplit(future.getTaskSession)
    waitForCompletionOrCancellation(future)
    val x = extractResult(aggregator)
    println(x)
    x
  }
}

trait GridTaskLinkedStatisticsAggregatorActor[T] extends StatisticsAggregatorActor[T] {
  import GridGainUtil._

  val future: GridOneWayTaskFuture

  def cancel() = {
    println("GridTaskLinkedStatisticsAggregatorActor.cancel")
    future.cancel
  }

  val taskId = future.getTaskSession.getId

}


abstract class MonteCarloSimulationGridJob[T](master: GridRichNode, workerId: Int) extends CancellableGridJob with GridTaskSessionAware with GridLoggerAware {
  val MaxSimulationBatchesPerWorker: Int = 10000

  override def logInfo(msg: => String) = super.logInfo("taskID: %s, workerID: %d || %s".format(taskSes.getId, workerId, msg))

  // This is required to force distributed class loading of the implementation class for the trait, before the job is cancelled.
  // TODO boil down a smaller example and report problem to GridGain.
  forceLoad

  def execute(): AnyRef = {
    logInfo("execute(), waiting for modelData attribute")
    val modelData: ModelData = taskSes.waitForAttribute(PiDigits.ModelDataAttributeKey)
    logInfo("got model data, starting simulation")
    for (batchId <- 1 to MaxSimulationBatchesPerWorker) {
      val localStatistics = simulationBatch(batchId)
      val message = LocalStatisticsMessage(taskSes.getId, (workerId, batchId), localStatistics)
      if (cancelled) {
        return null
      }

      try {
        logDebug("sending results from: %s".format(message))
        master !< message
      } catch {
        case e: GridRuntimeException =>
          logInfo("warning (cancellation may be in progress):" + e.getMessage)
          return null
      }
    }
    null
  }

  def simulationBatch(batchId: Int): T
}

case class LocalStatisticsMessage[T](taskID: UUID, statsID: Any, stats: T)

/**
 * An actor that listens for message of type `T`. If the message is related to the current
 * task, and has not been previously processed, it is handed to `process`.
 */
abstract class StatisticsAggregatorActor[T] extends GridListenActor[LocalStatisticsMessage[T]] {
  val processed = new HashSet[Any]()

  sealed abstract class StoppingDecision
  case object Stop extends StoppingDecision
  case object Continue extends StoppingDecision

  def receive(nodeId: UUID, localStats: LocalStatisticsMessage[T]) {
    if (localStats.taskID != taskId) {
      //      info("skipping, wrong task: %s".format(localStats))
    } else if (processed.contains(localStats.statsID)) {
      //      info("ignoring duplicate: %s".format(localStats))
    } else {
      processed += localStats.statsID
      process(localStats.stats) match {
        case Continue =>
        case Stop =>
          cancel
          stop() // It this is a trait: java.lang.IllegalAccessError: tried to access method org.gridgain.grid.GridListenActor.stop()V from class gridgaintest.StatisticsAggregatorActor$class
      }
    }
  }

  val taskId: UUID

  def process(stats: T): StoppingDecision

  def cancel(): Unit
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

trait CancellableGridJob extends GridJob {
  self: GridLoggerAware =>
  @volatile var cancelled: Boolean = false

  def cancel() = {
    cancelled = true
  }

  val forceLoad = ""
}

object GridGainUtil {
  type GridOneWayTask[T] = GridTask[T, Void]
  type GridOneWayTaskFuture = GridTaskFuture[Void]

  def splitOnlyGridTask[T](splitter: T => Seq[GridJob]): GridOneWayTask[T] = new GridTaskNoReduceSplitAdapter[T] {
    def split(gridSize: Int, workerIds: T): JCollection[_ <: GridJob] = {
      val jobs: JCollection[GridJob] = new JArrayList[GridJob]()
      for (job <- splitter(workerIds))
        jobs.add(job)
      jobs
    }
  }

  def waitForCompletionOrCancellation(f: GridOneWayTaskFuture) = try {
    f.get()
  } catch {
    case _: GridFutureCancelledException => // ignore
  }
}
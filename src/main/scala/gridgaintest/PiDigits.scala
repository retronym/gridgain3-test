package gridgaintest

import org.gridgain.scalar.scalar
import scalar._
import org.gridgain.grid._
import java.lang.Math
import java.security.SecureRandom
import java.util.concurrent.{TimeUnit, CountDownLatch}
import collection.mutable.HashSet
import java.util.{UUID, Collection => JCollection, ArrayList => JArrayList}
import resources.{GridTaskSessionResource, GridInstanceResource}
import actors.Futures._
import actors.Futures

case class ModelData(dummy: Array[Byte])
case class LocalStatistics(taskID: UUID, id: LocalStatisticsID, ics: Array[Boolean])
case class LocalStatisticsID(workerID: Int, batchID: Int)

object PiDigits {
  val MinSamples: Int = 128
  val MaxWorkers: Int = 16
  val RequiredVariance: Double = 0.00001
  val modelData = ModelData(new Array[Byte](8 * 1024 * 1024))

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

    grid.listenAsync(new GridListenActor[LocalStatistics]() {
      var batches = 0
      var total = 0
      var inCircle = 0
      val varianceStat = new VarianceOnlineStatistic
      val processed = new HashSet[LocalStatisticsID]()

      def receive(nodeId: UUID, localStats: LocalStatistics) {
        if (localStats.taskID != future.getTaskSession.getId) {
          println("skipping, wrong task: %s".format(localStats))
//          skip
        } else if (processed.contains(localStats.id)) {
          println("ignoring duplicate: %s".format(localStats))
        } else {
          processed += localStats.id

          for (ic <- localStats.ics) {
            total += 1
            if (ic) inCircle += 1
          }
          batches += 1
          varianceStat(4 * inCircle.toDouble / total.toDouble)
          println("processed: %s, pi=%f variance=%f".format(localStats, varianceStat.mean, varianceStat.variance))
          if (batches > MinSamples && varianceStat.variance < RequiredVariance) {
            println("close enough!")
            future.cancel
            stop()
          }
        }
      }
    })

    future.getTaskSession.setAttribute("modelData", modelData)

    try {
      future.get()
      println("completed")
    } catch {
      case _: GridFutureCancelledException =>
        println("stopped early")
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

class MonteCarloSimulationGridJob(master: GridRichNode, workerId: Int) extends GridJob {
  val MaxSimulationBatchesPerWorker: Int = 10000

  var taskSes: GridTaskSession = _

  @GridTaskSessionResource
  def setTaskSession(taskSes: GridTaskSession) = this.taskSes = taskSes;


  @volatile var cancelled: Boolean = false;
  val r = new SecureRandom()

  def cancel() = {
    println("cancel()")
    cancelled = true
  }

  def execute(): AnyRef = {
    println("waiting for modelData attribute")
    val modelData: ModelData = taskSes.waitForAttribute("modelData")
    println("got model data")

    println("Starting worker: %d, with model data: %s".format(workerId, modelData.getClass.getName))
    for (batchId <- 1 to MaxSimulationBatchesPerWorker) {
      val localStatistics = simulationBatch(batchId)
      if (cancelled) return null
      try {
        println("sending results from: %s".format(localStatistics.id))
        master !< localStatistics
      } catch {
        case e: GridRuntimeException =>
          println("warning (cancellation may be in progress):" + e.getMessage)
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

class VarianceOnlineStatistic {
  var n = 0
  var mean = 0d
  var variance = 0d
  var M2 = 0d

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

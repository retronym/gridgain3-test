package gridgaintest

import org.gridgain.scalar.scalar
import scalar._
import org.gridgain.grid._
import resources.GridInstanceResource
import java.lang.Math
import java.security.SecureRandom
import java.util.concurrent.{TimeUnit, CountDownLatch}
import collection.mutable.HashSet
import java.util.{UUID, Collection => JCollection, ArrayList => JArrayList}


case class ModelData(dummy: Array[Double])
case class LocalStatistics(id: LocalStatisticsID, ics: Array[Boolean])
case class LocalStatisticsID(workerID: Int, batchID: Int)

object PiDigits {
  val MinSamples: Int = 128
  val MaxWorkers: Int = 8
  val modelData = ModelData(new Array(1 * 1024 * 1024))

  def main(args: Array[String]) {
    scalar {
      (grid: Grid) =>
        val master = grid.localNode

        val latch = new CountDownLatch(1)
        val actor = new GridListenActor[LocalStatistics]() {
          var total = 0
          var inCircle = 0
          val varianceStat = new VarianceOnlineStatistic
          val processed = new HashSet[LocalStatisticsID]()

          def receive(nodeId: UUID, localStats: LocalStatistics) {
            if (processed.contains(localStats.id)) {
              println("ignoring duplicate: %s".format(localStats.id))
            } else {
              processed += localStats.id
              for (ic <- localStats.ics) {
                total += 1
                if (ic) inCircle += 1
                varianceStat(4 * inCircle.toDouble / total.toDouble)
              }
              println("pi=%f variance=%f".format(varianceStat.mean, varianceStat.variance))
              if (total > MinSamples && varianceStat.variance < 0.000005) {
                println("close enough!")
                latch.countDown
                stop()
              }
            }
          }
        }

        grid.listenAsync(actor)

        val workerIds = (1 to MaxWorkers).toList

        val future: GridTaskFuture[Void] = grid.execute(new MonteCarloSimulationTask(master, modelData), workerIds)
        future.listenAsync(
          (f: GridFuture[Void]) => {
            println(if (f.isCancelled) "cancelled" else "max simulations reached")
            latch.countDown
            true
          })
        latch.await
        future.cancel
        //      TimeUnit.SECONDS.sleep(5)
        ()
    }
  }

  class MonteCarloSimulationTask(master: GridRichNode, modelData: ModelData) extends GridTaskNoReduceSplitAdapter[List[Int]] {
    /**
     * Splits input arguments into collection of closures.
     */
    def split(gridSize: Int, workerIds: List[Int]): JCollection[_ <: GridJob] = {

      val jobs: JCollection[GridJob] = new JArrayList[GridJob]()

      for ((w: Int) <- workerIds)
        jobs.add(new MonteCarloSimulationGridJob(master, w, modelData))
      jobs
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
}

class MonteCarloSimulationGridJob(master: GridRichNode, workerId: Int, modelData: ModelData) extends GridJob {
  val MaxSimulationBatchesPerWorker: Int = 10000
  
  @volatile var cancelled: Boolean = false;
  val r = new SecureRandom()

  def cancel() = {
    println("cancel()")
    cancelled = true
  }

  def execute(): AnyRef = {
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
      val inCircle: Boolean = (x * x + y * y) < 1d
      inCircle
    }
    LocalStatistics(LocalStatisticsID(workerId, batchId), ics.toArray)
  }
}

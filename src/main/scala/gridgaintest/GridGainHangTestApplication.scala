package gridgaintest

import java.util.UUID
import org.gridgain.grid.resources.GridTaskSessionResource
import org.gridgain.grid._
import org.gridgain.scalar.scalar
import actors.Futures._
import java.util.concurrent.TimeUnit

/**
 * An attempt to reproduce the hang on shutdown I see with PiSimApplication. Doesn't hang yet :(
 */
object GridGainHangTestApplication {
  def main(args: Array[String]) {
    for (i <- 0 until 10) scalar { (grid: Grid) =>
      def runTask = {
        val master = grid.localNode
        val task = new GridTaskNoReduceSplitAdapter[AnyRef] {
          def split(gridSize: Int, t: AnyRef): java.util.Collection[_ <: GridJob] = {
            val jobs: java.util.Collection[GridJob] = new java.util.ArrayList[GridJob]()
            for (job <- (0 until 8))
              jobs.add(new GridJob {
                println("worker created")
                @volatile var cancelled = false

                def cancel = {cancelled = true}

                @GridTaskSessionResource
                def setSession(s: GridTaskSession) {session = s}

                var session: GridTaskSession = _

                def execute = {
                  println("worker ready in session: " + session.getId)
                  session.waitForAttribute("start")
                  println("worker started in session: " + session.getId)
                  def execute(i: Int): Unit = if (!cancelled && i <= 10) {
                    println("sending message to master: %d".format(i))
                    master.send(new Object(), null)
                    try {
                      TimeUnit.MILLISECONDS.sleep(100)
                    } catch {
                      case _: InterruptedException => // wake up
                    }
                    execute(i + 1)
                  }
                  execute(0)
                  println("worker done")
                  null
                }
              })
            println("split into jobs: " + jobs)
            jobs
          }
        }

        val gridTaskFuture: GridTaskFuture[Void] = grid.execute(task, null)

        master.listenAsync(new GridListenActor[AnyRef] {
          var counter = 0

          def receive(nodeId: UUID, recvMsg: AnyRef) = {
            println("received: " + recvMsg)
            counter += 1
            if (counter > 20) {
              println("cancelling task and deregistering actor")
              gridTaskFuture.cancel
              stop
            }
          }
        })
        gridTaskFuture.getTaskSession.setAttribute("start", "start")
        println("waiting on gridTaskFuture")
        try {
          gridTaskFuture.get
        } catch {
          case _: GridFutureCancelledException => // ignore
        }
        println("gridTaskFuture completed")
      }
      Seq(future(runTask), future(runTask)).map(_())
    }
  }
}

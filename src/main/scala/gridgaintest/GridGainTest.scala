package gridgaintest

import org.gridgain.scalar.scalar
import scalar._
import java.util.UUID
import java.util.concurrent.CountDownLatch
import org.gridgain.grid.{Grid, GridListenActor}

object GridGainTest {
  def main(args: Array[String]) {
    println("hello world")
    scalar {
      (grid: Grid) =>
        val a = grid.localNode
        val b = grid.remoteNodes().iterator.next
        //        grid <!
        ()
    }
  }
}


object ScalarPingPongExample {
  def main(args: Array[String]) {
    scalar {
      (grid: Grid) =>
        if (grid.nodes().size < 2) {
          error("I need a partner to play a ping pong!")

          return
        }

        // Pick first remote node as a partner.
        val a = grid.localNode
        val b = grid.remoteNodes().iterator.next

        // Note that both 'a' and 'b' will always point to
        // same nodes regardless of whether they were implicitly
        // serialized and deserialized on another node as part of
        // anonymous closure's state during its remote execution.
        case class Foo(a: Seq[Int])

        val foo: Foo = new Foo(Array(1, 2, 3))

        // Set up remote player.
        b.remoteListenAsync(a, new GridListenActor[String]() {
          def receive(nodeId: UUID, msg: String) {
            println(foo)
            println(msg)

            msg match {
              case "PING" => respond("PONG")
              case "STOP" => stop
            }
          }
        })

        val MAX_PLAYS = 10

        val cnt = new CountDownLatch(MAX_PLAYS)

        // Set up local player.
        b.listenAsync(new GridListenActor[String]() {
          def receive(nodeId: UUID, msg: String) {
            println(msg)

            if (cnt.getCount() == 1)
              stop("STOP")
            else
              msg match {
                case "PONG" => respond("PING")
              }

            cnt.countDown();
          }
        })

        // Serve!
        b !< "PING"

        // Wait til the game is over.
        cnt.await()
    }
  }
}
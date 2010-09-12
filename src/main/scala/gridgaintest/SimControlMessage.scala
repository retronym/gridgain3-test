package gridgaintest

sealed abstract class SimControlMessage

case object Stop extends SimControlMessage
case object Continue extends SimControlMessage
case class BroadcastAndContinue[T](t: T) extends SimControlMessage

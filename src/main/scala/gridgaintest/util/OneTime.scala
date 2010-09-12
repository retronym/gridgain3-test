package gridgaintest.util

import collection.mutable.HashSet

class OneTime[T, U](keyExtract: T => Any, valueExtract: T => U) {
  private val processed = new HashSet[Any]()

  /**
   * If keyExtract(t) has not been previously processed, execute f(valueExtract(t)),
   * otherwise ignore it.
   */
  def apply(t: T)(f: U => Unit) = {
    val key = keyExtract(t)
    val alreadyProcessed = processed.contains(key)
    if (!alreadyProcessed) {
      processed += key
      f(valueExtract(t))
    }
  }
}
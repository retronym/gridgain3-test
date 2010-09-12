package gridgaintest.util

final class MeanVarianceOnlineStatistic {
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
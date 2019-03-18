

/**
 * @author KwangMin
 */
import Driver.earthR
import Driver.threshold

class Distance_Handler extends Serializable {

  // arc length
  def arc_length(one: String, two: String): Double = {
    val x: Double = one.toDouble
    val y: Double = two.toDouble
    val ang: Double = x - y

    // 2 * Pi * R * ang / 360
    val result: Double = earthR * 2 * math.Pi * ang / 360

    return result
  }

  // estimate distance
  def distance(one: Map[String, String], two: Map[String, String]): Double = {
    val lat1: String = one("lat")
    val lat2: String = two("lat")
    val lon1: String = one("lon")
    val lon2: String = two("lon")

    val x: Double = arc_length(lat1, lat2)
    val y: Double = arc_length(lon1, lon2)

    // root(x^2 + y^2)
    val result: Double = scala.math.hypot(x, y)

    return result
  }

  // check distance of two GPS
  def is_close(one: Map[String, String], two: Map[String, String]): Boolean = {
    val dis = distance(one, two)

    if (dis <= threshold) {
      return true
    } else {
      return false
    }
  }
}
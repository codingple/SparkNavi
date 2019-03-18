

/**
 * @author KwangMin
 */
import scala.util.control.Breaks
import Driver.direction_checker

class Check_Reverse_Direction extends Serializable {
  val dis = new Distance_Handler()

  def apply(list1: List[Map[String, String]], list2: List[Map[String, String]], point1: Map[String, String], point2: Map[String, String]): Boolean = {

    var criterion = dis.distance(point1, point2)
    var index1 = list1.indexOf(point1)
    var index2 = list2.indexOf(point2)
    var node1 = point1
    var node2 = point2
    var result: Boolean = true
    var mode: Int = 0
    val loop = new Breaks

    // check the mode
    // mode 0 : increase -> decrease
    // mode 1 : decrease -> increase
    if (dis.distance(list1(index1 - 1), point2) < criterion)
      mode = 1;

    // mode 0
    if (mode == 0) {
      for (i <- 1 to direction_checker) {
        // list1's turn
        index1 -= 1
        node1 = list1(index1)

        var increase = dis.distance(node1, node2)

        // have to increase
        if (increase < criterion)
          return result;

        // list2's turn
        criterion = increase
        index2 += 1
        node2 = list2(index2)

        var decrease = dis.distance(node1, node2)

        // have to decrease
        if (decrease > criterion)
          return result;

        criterion = decrease
      }
    } // end of mode 0
    // mode 1
    else if (mode == 1) {
      for (i <- 1 to direction_checker) {
        // list1's turn
        index1 -= 1
        node1 = list1(index1)

        var decrease = dis.distance(node1, node2)

        // have to decrease
        if (decrease > criterion)
          return result;

        // list2's turn
        criterion = decrease
        index2 += 1
        node2 = list2(index2)

        var increase = dis.distance(node1, node2)

        // have to increase
        if (increase < criterion)
          return result;

        criterion = increase
      }
    } // end of mode 0

    result = false
    return result
  }
}
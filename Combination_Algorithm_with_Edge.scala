

/**
 * @author KwangMin
 */
import java.io.File
import Driver.direction_checker
import Driver.sc

class Combination_Algorithm_with_Edge extends Serializable {
  //val log = Log
  val check = new Check_Reverse_Direction()
  val dis = new Distance_Handler()

  def apply(list1: List[Map[String, String]], list2: List[Map[String, String]], point1: Map[String, String], point2: Map[String, String]) : Int = {
    var index1 = 0
    var index2 = 0

    // head=1, tail=2, middle=3 
    // point1
    if (list1.indexOf(point1) < direction_checker)
      index1 = 1;
    else if (list1.indexOf(point1) > list1.length - (direction_checker + 1))
      index1 = 2;
    else
      index1 = 3;

    // point2
    if (list2.indexOf(point2) < direction_checker)
      index2 = 1;
    else if (list2.indexOf(point2) > list2.length - (direction_checker + 1))
      index2 = 2;
    else
      index2 = 3;

    // 2 * 2 cases : (1,2) (2,1) (2,3) (3,2) can generate a new route
    // (head,tail), (middle,tail)
    if ((index1 == 1 && index2 == 2) || (index1 == 3 && index2 == 2)) {

      // check direction
      if (check(list2, list1, point2, point1)) {
        return 1
      }
    } // (tail,head), (tail,middle)
    else if ((index1 == 2 && index2 == 1) || (index1 == 2 && index2 == 3)) {

      // check direction
      if (check(list1, list2, point1, point2)) {
        return 2
        //println("Edge check : " + log.check())
        //log.edgeAdd()
      }
      
    }
    return 0
  }
}
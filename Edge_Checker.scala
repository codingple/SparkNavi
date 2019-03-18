

/**
 * @author KwangMin
 */
import Driver.direction_checker

class Edge_Checker extends Serializable {
  
  def apply(point1: Map[String, String], point2: Map[String, String], tra1: List[Map[String, String]], tra2: List[Map[String, String]]): Boolean = {
    if( tra1.indexOf(point1) < direction_checker
        || tra1.indexOf(point1) > tra1.length - (direction_checker + 1)
        || tra2.indexOf(point2) < direction_checker
        || tra2.indexOf(point2) > tra2.length - (direction_checker + 1)){
      
      return true
      
      }
    
    else{
      
      return false
    }
  }
}
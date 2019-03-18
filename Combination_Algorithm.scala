

/**
 * @author KwangMin
 */
import java.io.File
import Driver.direction_checker

class Combination_Algorithm extends Serializable {
  //val log = Log
  val check_reverse = new Check_Reverse_Direction()
  val check_same = new Check_Same_Direction()
  var out = 0

  // 2 * 2 cases : TTT, TFT or FTT, TTF, FFT(rarely)
  def apply(tra1: List[Map[String, String]], tra2: List[Map[String, String]], point1: Map[String, String], point2: Map[String, String],
            file1: String, file2: String)
  : List[(String, String, Map[String, String], Map[String, String])] = {
    val one = check_reverse(tra1, tra2, point1, point2)
    val two = check_same(tra1, tra2, point1, point2)
    val three = check_reverse(tra2, tra1, point2, point1)
    val four = check_same(tra2, tra1, point2, point1)
    val five = same(two, four)
    var result: List[(String, String, Map[String, String], Map[String, String])] = List()
    out = 0

    // TTT
    if (one && two && five) {
      result = result :+ (file1, file2, point1, point2)
      result = result :+ (file2, file1, point2, point1)
      out = 1
      //log.tttAdd()
      //println("New Route Generated : " + log.check())
      return result
    } // TFT or FTT
    else if ((one || three) && five) {
      if (one) {
        result = result :+ (file1, file2, point1, point2)
        out = 2
        //log.tftAdd()
        //println("New Route Generated : " + log.check())
        return result
      } else {
        result = result :+ (file2, file1, point2, point1)
        out = 3
        //log.fttAdd()
        //println("New Route Generated : " + log.check())
        return result
      }
    } // TTF
    else if (one && three && !five) {
      out = 4
      //log.ttfAdd()
      return result
    } // FFT
    else if (!(one || three) && five) {
      out = 5
      //log.fftAdd()
      return result
    } else
      return result
  }
  
  def getout(): Int = {
    return this.out
  }

  def same(two: Boolean, four: Boolean): Boolean = {
    return (two || four)
  }
}
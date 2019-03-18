

/**
 * @author KwangMin
 */
import scala.util.control.Breaks
import java.io.File
import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd
import org.apache.spark.rdd._
import Driver.sc
import Driver.partition

object Route_Generator extends Serializable {
  type trajectory = (String, List[Map[String, String]])
  type point = Map[String, String]
  type relation = (String, String, point, point)
  type relation2 = (String, String, point, point)

  def func1(tras: List[trajectory], tra: trajectory): List[(trajectory, trajectory)] = {
    val len = tras.length
    val index = tras.indexOf(tra)
    var result: List[(trajectory, trajectory)] = List()
    val list = tras.drop(index + 1).toIterator

    while (list.hasNext) {
      val t = list.next()
      val tmp = (tra, t)
      result = result :+ tmp
    }

    return result
  }

  def func2(coup: (trajectory, trajectory)): List[relation] = {
    var result: List[relation] = List()
    val iter1 = coup._1._2.toIterator
    while (iter1.hasNext) {
      val point1 = iter1.next()
      val iter2 = coup._2._2.toIterator
      while (iter2.hasNext) {
        val point2 = iter2.next()
        result = result :+ (coup._1._1, coup._2._1, point1, point2)
      }
    }
    return result
  }

  def func3(tras: List[trajectory], rel: relation): List[relation2] = {
    var dis = new Distance_Handler()
    var edgecheck = new Edge_Checker()
    var combi = new Combination_Algorithm()
    var combiedge = new Combination_Algorithm_with_Edge()
    var check = new Check_Reverse_Direction()
    var checksame = new Check_Same_Direction()
    var result: List[relation2] = List()

    val f1 = rel._1
    val f2 = rel._2
    val tra1 = tras.filter { x => (x._1 == f1) }(0)._2
    val tra2 = tras.filter { x => (x._1 == f2) }(0)._2
    val point1 = rel._3
    val point2 = rel._4

    if (dis.is_close(point1, point2)) {
      if (edgecheck(point1, point2, tra1, tra2)) {
        combiedge(tra1, tra2, point1, point2)
      } else {
        val tmp = combi(tra1, tra2, point1, point2, f1, f2)
        if ((!tmp.isEmpty) && (!result.contains(tmp))) {
          result = result ::: tmp
        }
      }
    }
    return result
  }

  def func4(iter: Iterator[(trajectory, trajectory)]): Iterator[relation2] = {
    var result: List[relation2] = List()
    var dis = new Distance_Handler()
    var edgecheck = new Edge_Checker()
    var combi = new Combination_Algorithm()
    var combiedge = new Combination_Algorithm_with_Edge()
    var tra1: List[Map[String, String]] = List()
    var tra2: List[Map[String, String]] = List()
    var point1: Map[String, String] = Map()
    var point2: Map[String, String] = Map()
    var iter1: Iterator[Map[String, String]] = Iterator()
    var iter2: Iterator[Map[String, String]] = Iterator()

    while (iter.hasNext) {
      val coup = iter.next()
      tra1 = coup._1._2
      iter1 = tra1.toIterator
      while (iter1.hasNext) {
        point1 = iter1.next()
        tra2 = coup._2._2
        iter2 = tra2.toIterator
        while (iter2.hasNext) {
          point2 = iter2.next()

          if (dis.is_close(point1, point2)) {
            if (edgecheck(point1, point2, coup._1._2, coup._2._2)) {
              val get = combiedge(coup._1._2, coup._2._2, point1, point2)
              if (get == 1)
                close_distance();
              if (get == 2)
                end();
            } else {
              val tmp = combi(coup._1._2, coup._2._2, point1, point2, coup._1._1, coup._2._1)
              if ((!tmp.isEmpty) && (!result.contains(tmp))) {
                result = result ::: tmp
              }
              val get = combi.getout()
              if (get == 1)
                close_distance();
              else if (get == 2)
                reverse_direction();
              else if (get == 3)
                close_distance();
              else if (get == 4)
                same_direction();
              else if (get == 5)
                reverse_direction();
            }
          }
        }
      }
    }

    def end() {
      while (iter1.hasNext) point1 = iter1.next();
      while (iter2.hasNext) point2 = iter2.next();
    }

    // merge two results of check_same
    def same(two: Boolean, four: Boolean): Boolean = {

      return (two || four)
    }

    def close_distance() {
      val loop = new Breaks()

      loop.breakable {
        while (true) {
          // num1
          if (iter2.hasNext) {
            point2 = iter2.next()

            // num3
            if (dis.is_close(point1, point2)) {} // nothing

            // num4
            else {

              // num5
              if (iter1.hasNext) {
                point1 = iter1.next()

                // num7
                if (dis.is_close(point1, point2)) {} // nothing

                // num8
                else {

                  loop.break()
                }
              } // num6
              else {
                loop.break()
              }
            }
          } // num2
          else {

            // num9
            while (iter1.hasNext) {
              point1 = iter1.next()

              // num11
              if (dis.is_close(point1, point2)) {} // nothing

              // num12
              loop.break()
            }

            // num10
            loop.break()
          }
        } // end of while
      } // end of breakable and progress
    }

    // back to the close points after progress
    def same_direction() {
      var tmp_iter1 = iter1
      var tmp_iter2 = iter2
      val loop1 = new Breaks

      loop1.breakable {
        while (true) {
          // num1
          if (iter2.hasNext) {
            tmp_iter2 = iter2
            point2 = iter2.next()

            // num3
            if (dis.is_close(point1, point2)) {} // nothing

            // num4
            else {

              // num5
              if (iter1.hasNext) {
                tmp_iter1 = iter1
                point1 = iter1.next()

                // num7
                if (dis.is_close(point1, point2)) {} // nothing

                // num8
                else {
                  var index1 = tra1.indexOf(point1)
                  point1 = tra1(index1 - 1)
                  var index2 = tra2.indexOf(point2)
                  point2 = tra2(index2 - 1)
                  iter1 = tmp_iter1
                  iter2 = tmp_iter2
                  loop1.break()
                }
              } // num6
              else {
                var index2 = tra2.indexOf(point2)
                point2 = tra2(index2 - 1)
                iter2 = tmp_iter2
                loop1.break()
              }
            }
          } // num2
          else {
            // num9
            while (iter1.hasNext) {
              tmp_iter1 = iter1
              point1 = iter1.next()

              // num11
              if (dis.is_close(point1, point2)) {} // nothing

              // num12
              var index1 = tra1.indexOf(point1)
              point1 = tra1(index1 - 1)
              iter1 = tmp_iter1
              loop1.break()
            }

            // num10
            loop1.break()
          }
        } // end of while
      } // end of breakable and progress_same
    }

    def reverse_direction() {
      val loop2 = new Breaks

      loop2.breakable {
        var tmp_iter1 = iter1
        var index2 = tra2.indexOf(point2)
        var len2 = tra2.length - 1

        while (true) {
          // num1
          if (index2 > 0) {
            index2 -= 1
            point2 = tra2(index2)

            // num3
            if (dis.is_close(point1, point2)) {} // nothing

            // num4
            else {
              // num5
              if (iter1.hasNext) {
                tmp_iter1 = iter1
                point1 = iter1.next()

                // num7
                if (dis.is_close(point1, point2)) {} // nothing

                // num8
                else {
                  var index1 = tra1.indexOf(point1)
                  point1 = tra1(index1 - 1)
                  point2 = tra2(index2 + 1)
                  iter1 = tmp_iter1
                  var tmp_iter2 = tra2.iterator
                  for (z <- 0 to (index2 + 1))
                    tmp_iter2.next();
                  iter2 = tmp_iter2
                  loop2.break()
                }
              } // num6
              else {
                point2 = tra2(index2 + 1)
                var tmp_iter2 = tra2.iterator
                for (z <- 0 to (index2 + 1))
                  tmp_iter2.next();
                iter2 = tmp_iter2
                loop2.break()
              }
            }
          } // num2
          else {

            // num9
            while (iter1.hasNext) {
              tmp_iter1 = iter1
              point1 = iter1.next()

              // num11
              if (dis.is_close(point1, point2)) {} // nothing

              // num12
              var index1 = tra1.indexOf(point1)
              point1 = tra1(index1 - 1)
              iter1 = tmp_iter1
              loop2.break()
            }

            // num10
            loop2.break()
          }
        } // end of while
      } // end of breakable and progress_opposite
    }

    return result.toIterator
  }

  def apply(tras: List[trajectory]): RDD[relation2] = {
    val rdd = sc.parallelize(tras)
    .repartition(3)
      .coalesce(partition, false)
    var len = 0

    val tmp1 = rdd
      .map { x => func1(tras, x) }
      .flatMap(identity)

    val tmp2 = tmp1
        .repartition(3)
      .coalesce(partition, false)
      .mapPartitions(func4, true)
    /*
    val tmp2 = tmp1
      .map(x => func2(x))
      .flatMap { identity }
      .map { x => func3(tras, x) }
      .flatMap(identity)
      .collect()*/

    return tmp2
  }

  /*
  // input trajectories
  def apply(trajectory1: (String, List[Map[String, String]]), trajectory2: (String, List[Map[String, String]])): List[(String, String, Map[String, String], Map[String, String])] = {
    tra1 = trajectory1._2.toList
    tra2 = trajectory2._2.toList
    val f1 = trajectory1._1.toString()
    val f2 = trajectory2._1.toString()

    var result: List[(String, String, Map[String, String], Map[String, String])] = List()
    iter1 = tra1.toIterator

    while (iter1.hasNext) {
      point1 = iter1.next()
      iter2 = tra2.iterator

      while (iter2.hasNext) {
        point2 = iter2.next()

        // two GPSs are close
        if (dis.is_close(point1, point2)) {
          if (edgecheck(point1, point2, tra1, tra2)) {
            combiedge(tra1, tra2, point1, point2, f1, f2)
          } else {
            val tmp = combi(tra1, tra2, point1, point2, f1, f2)
            if (!tmp.isEmpty) {
              if (!result.contains(tmp))
                result = result ::: tmp
            }
          }

        } // end of is_close
      } // end of while iter2
    } // end of while iter1

    return result
  } // end of apply
*/
}
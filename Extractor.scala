

/**
 * @author KwangMin
 */
import scala.io.Source
import java.io.File
import org.apache.hadoop.fs._;
import org.apache.hadoop.conf._;
import org.apache.hadoop.io._;
import org.apache.hadoop.mapred._;
import org.apache.hadoop.util._;
import java.net.URI
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd
import org.apache.spark.rdd._
import Driver.sc
import Driver.partition
import Driver.Hdfs

/* Extractor is supposed to extract trajectory informations
 * such as latitude, longitude, time.
 * In addition, This object extracts the information
 * except equal located GPSs and error GPS via <speed> tag 
 */

object Extractor extends Serializable {

  def apply(files: List[String]): List[List[Map[String, String]]] = {
    var result: List[List[Map[String, String]]] = List()
    val rdd = sc.parallelize(files, 3)
    result = rdd.map(x => get_info(x)).collect().toList
    return result
  }

  // return info
  def get_info(file: String): List[Map[String, String]] = {
    return remove(extract_trajectory(file))
  }

  def get_info_rdd2(file: String): List[Map[String, String]] = {
    return remove(rdd_extract(file))
  }

  def get_info_rdd(rdd: RDD[(String, String)]): List[(String, List[Map[String, String]])] = {
    val tmp = rdd.map { case (x, y) => (x, remove(rdd_extract(y))) }
    val res = tmp.collect().toList
    return res
  }

  // remove equal located GPSs
  def remove(origin: List[Map[String, String]]): List[Map[String, String]] = {
    var list = origin
    val iter = origin.toIterator
    var point1: Map[String, String] = iter.next()
    var point2: Map[String, String] = Map()

    while (iter.hasNext) {
      point2 = iter.next()

      // two points have same location
      if (point1("lat").equals(point2("lat")) && point1("lon").equals(point2("lon"))) {
        list = list.diff(List[Map[String, String]](point2))
      } else {
        point1 = point2
      }
    }

    return list

  }

  def rdd_extract(cont: String): List[Map[String, String]] = {
    var trajectory: List[Map[String, String]] = List()
    var node: Map[String, String] = Map()

    val lat_pattern = "lat=\".*\" ".r
    val lon_pattern = "lon=\".*\"".r
    val time_pattern = "<time>.*</time>".r

    // read file
    val file = cont.split('\n').toIterator

    while (file.hasNext) {
      var line = file.next()

      // latitude
      if (!lat_pattern.findAllIn(line).isEmpty) {
        val text = lat_pattern
          .findAllIn(line).next().split("\"")
        val result = text.apply(1)

        node = Map("lat" -> result)
      }

      //longitude
      if (!lon_pattern.findAllIn(line).isEmpty) {
        val text = lon_pattern
          .findAllIn(line).next().split("\"")
        val result = text.apply(1)

        node = node + ("lon" -> result)
      }

      // time
      if (!time_pattern.findAllIn(line).isEmpty) {
        val text = time_pattern
          .findAllIn(line).next().split(">")
        val result = text
          .apply(1).split("<").apply(0)

        node = node + ("time" -> result)
      }

      // speed
      if (line.contains("<speed>")) {
        trajectory = node :: trajectory
      }
    } // end of while
    return trajectory.reverse
  }

  // extract trajectory
  def extract_trajectory(file: String): List[Map[String, String]] = {
    var trajectory: List[Map[String, String]] = List()
    var node: Map[String, String] = Map()

    val lat_pattern = "lat=\".*\" ".r
    val lon_pattern = "lon=\".*\"".r
    val time_pattern = "<time>.*</time>".r

    // read file
    val fs = FileSystem.get(new URI(Hdfs), new Configuration())
    val pt = new Path(file);
    val br = new BufferedReader(new InputStreamReader(fs.open(pt)))
    var line = br.readLine()

    while (line != null) {

      // latitude
      if (!lat_pattern.findAllIn(line).isEmpty) {
        val text = lat_pattern
          .findAllIn(line).next().split("\"")
        val result = text.apply(1)

        node = Map("lat" -> result)
      }

      //longitude
      if (!lon_pattern.findAllIn(line).isEmpty) {
        val text = lon_pattern
          .findAllIn(line).next().split("\"")
        val result = text.apply(1)

        node = node + ("lon" -> result)
      }

      // time
      if (!time_pattern.findAllIn(line).isEmpty) {
        val text = time_pattern
          .findAllIn(line).next().split(">")
        val result = text
          .apply(1).split("<").apply(0)

        node = node + ("time" -> result)
      }

      // speed
      if (line.contains("<speed>")) {
        trajectory = node :: trajectory
      }

      line = br.readLine()
    } // end of while

    br.close()
    return trajectory.reverse
  }
}


/**
 * @author KwangMin
 */

import scala.util.control.Breaks
import java.text.SimpleDateFormat
import java.util.Calendar
import Driver.des
import Driver.sc
import Driver.partition
import org.apache.hadoop.fs._;
import org.apache.hadoop.conf._;
import org.apache.hadoop.io._;
import org.apache.hadoop.mapred._;
import org.apache.hadoop.util._;
import java.net.URI
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd
import org.apache.spark.rdd._

object GPX_Creator extends Serializable {
  val format = new SimpleDateFormat("HHmmssSSS")
  val fs = FileSystem.get(new URI("hdfs://avocado:9000"), new Configuration())

  def rdd_apply(files: RDD[(String, String)], relations: List[(String, String, Map[String, String], Map[String, String])]) {
    val log = Log
    val loop = new Breaks()
    val lat_pattern = "lat=\".*\" ".r
    val lon_pattern = "lon=\".*\"".r

    val iter = relations.toIterator
    val all = relations.length
    var num = 0

    while (iter.hasNext) {
      val relation = iter.next()
      val file1 = files.filter(f => f._1 == relation._1).collect()(0)._2
      val file2 = files.filter(f => f._1 == relation._2).collect()(0)._2
      val point1 = relation._3
      val point2 = relation._4

      // writer file
      val filename = format.format(Calendar.getInstance().getTime())
      val pt3 = new Path(des + "/" + filename + ".gpx")
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(pt3, true)))

      // read file
      var reader = file1.split('\n').toIterator

      // write head part
      loop.breakable {
        while (reader.hasNext) {
          var line = reader.next()
          // compare latitude
          if (!lat_pattern.findAllIn(line).isEmpty) {
            val text = lat_pattern
              .findAllIn(line).next().split("\"")
            val lat_res = text.apply(1)

            // compare longitude
            if (point1("lat").equals(lat_res)) {
              val text = lon_pattern
                .findAllIn(line).next().split("\"")
              val lon_res = text.apply(1)

              // write last point
              if (point1("lon").equals(lon_res)) {
                while (true) {
                  writer.write(line + "\n")
                  line = reader.next()
                  if (line.contains("</trkpt>")) {
                    writer.write(line + "\n")
                    loop.break()
                  } // end of if
                } // end of while (true)
              } // end of if (lon_res)
            } // end of if (lat_res)
          } // end of if (isEmplty)
          writer.write(line + "\n")
        } // end of while (reader)
      } // end of breakable
      // write middle GPS
      val middle = generate_middle(point1, point2)

      //write middle
      writer.write("<trkpt lat=\"" + middle("lat") + "\" lon=\"" + middle("lon") + "\">\n")
      writer.write("<time>" + middle("time") + "</time>\n")
      writer.write("</trkpt>\n")

      // write tail part
      reader = file2.split('\n').toIterator

      while (reader.hasNext) {
        var line = reader.next()
        // compare latitude
        if (!lat_pattern.findAllIn(line).isEmpty) {
          val text = lat_pattern
            .findAllIn(line).next().split("\"")
          val lat_res = text.apply(1)

          // compare longitude
          if (point2("lat").equals(lat_res)) {
            val text = lon_pattern
              .findAllIn(line).next().split("\"")
            val lon_res = text.apply(1)

            // write until last
            if (point2("lon").equals(lon_res)) {
              while (reader.hasNext) {
                writer.write(line + "\n")
                line = reader.next()
              }
              writer.write(line + "\n")
            }
          }
        }
      }
      writer.close()
      num += 1
      println("GPX file created " + num + "/" + all + " : " + log.check())
    }
  }

  def apply(relations: RDD[(String, String, Map[String, String], Map[String, String])]): Int = {
    val log = Log
    val loop = new Breaks()
    val lat_pattern = "lat=\".*\" ".r
    val lon_pattern = "lon=\".*\"".r

    val iter = relations
    .repartition(3)
    .coalesce(partition, true)
    .collect()
    .toIterator

    val all = iter.length
    var num = 0

    while (iter.hasNext) {
      val relation = iter.next()
      val file1 = relation._1
      val file2 = relation._2
      val point1 = relation._3
      val point2 = relation._4

      // writer file
      val filename = format.format(Calendar.getInstance().getTime())
      val pt3 = new Path(des + "/" + filename + ".gpx")
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(pt3, true)));

      // read file
      val pt = new Path(file1);
      val br = new BufferedReader(new InputStreamReader(fs.open(pt)))
      var line = br.readLine()

      // write head part
      loop.breakable {
        while (line != null) {
          // compare latitude
          if (!lat_pattern.findAllIn(line).isEmpty) {
            val text = lat_pattern
              .findAllIn(line).next().split("\"")
            val lat_res = text.apply(1)

            // compare longitude
            if (point1("lat").equals(lat_res)) {
              val text = lon_pattern
                .findAllIn(line).next().split("\"")
              val lon_res = text.apply(1)

              // write last point
              if (point1("lon").equals(lon_res)) {
                while (true) {
                  writer.write(line + "\n")
                  line = br.readLine()
                  if (line.contains("</trkpt>")) {
                    writer.write(line + "\n")
                    loop.break()
                  } // end of if
                } // end of while (true)
              } // end of if (lon_res)
            } // end of if (lat_res)
          } // end of if (isEmplty)
          writer.write(line + "\n")
          line = br.readLine()
        } // end of while (reader)
      } // end of breakable

      br.close()
      // write middle GPS
      val middle = generate_middle(point1, point2)

      //write middle
      writer.write("<trkpt lat=\"" + middle("lat") + "\" lon=\"" + middle("lon") + "\">\n")
      writer.write("<time>" + middle("time") + "</time>\n")
      writer.write("</trkpt>\n")

      // write tail part
      val pt2 = new Path(file2);
      val br2 = new BufferedReader(new InputStreamReader(fs.open(pt2)))
      line = br2.readLine()

      while (line != null) {
        // compare latitude
        if (!lat_pattern.findAllIn(line).isEmpty) {
          val text = lat_pattern
            .findAllIn(line).next().split("\"")
          val lat_res = text.apply(1)

          // compare longitude
          if (point2("lat").equals(lat_res)) {
            val text = lon_pattern
              .findAllIn(line).next().split("\"")
            val lon_res = text.apply(1)

            // write until last
            if (point2("lon").equals(lon_res)) {
              while (line != null) {
                writer.write(line + "\n")
                line = br2.readLine()
              }
            }
          }
        }
        line = br2.readLine()
      }
      writer.close()
      br2.close()
      num += 1
      println("GPX file created " + num + "/" + all + " : " + log.check())
    }
    return num
  }

  // generate middle GPS of two ones
  def generate_middle(point1: Map[String, String], point2: Map[String, String]): Map[String, String] = {
    val lat = (point1("lat").toDouble + point2("lat").toDouble) / 2
    val lon = (point1("lon").toDouble + point2("lon").toDouble) / 2

    val result: Map[String, String] = Map("lat" -> lat.toString(), "lon" -> lon.toString(), "time" -> "null")

    return result
  }
}
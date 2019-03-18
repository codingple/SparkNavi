

/**
 * @author user
 */

import scala.io.Source
import scala.Double
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.io.File
import scala.collection.mutable
import scala.collection.immutable
import scala.util.matching
import scala.math
import scala.util.control.Breaks
import java.text.SimpleDateFormat
import java.util.Calendar
import java.net.URI
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd
import org.apache.spark.rdd._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._
import org.apache.spark.storage._

val SparkServer = "192.168.0.1:7077"
val Hdfs = "hdfs://avocado:9000"

object Driver {
  val earthR: Double = 6378100.0 // meter
  val threshold: Double = 10.0
  val direction_checker: Int = 3
  val app = "Trajectory_Maker"
  val master = "spark://"+SparkServer
  val conf = new SparkConf().setAppName(app).setMaster(master)
    .set("spark.akka.timeout", "1000")
    .set("spark.akka.heartbeat.pauses", "60000")
    .set("spark.akka.failure-detector.threshold", "3000.0")
    .set("spark.akka.heartbeat.interval", "10000")
    .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    .set("spark.akka.frameSize", "500") // workers should be able to send bigger messages
    .set("spark.akka.askTimeout", "30") // high CPU/IO load
    .set("spark.executor.memory", "6G")
    .set("spark.driver.memory", "3G")
    .set("spark.storage.memoryFraction", "1")
    .set("spark.num.executors", "3")
  val sc = new SparkContext(conf)
  var logd = Hdfs+"trajectory/logd"
  var counter = 0
  var dir = Hdfs+"trajectory/" + counter
  var des = Hdfs+"trajectory/" + (counter + 1)
  val partition = 3

  def main(args: Array[String]) {

    val log = Log
    val ext = Extractor
    val gpx = GPX_Creator
    val rougen = Route_Generator
    val format = new SimpleDateFormat("HHmmssSSS")
    var numf: Int = 1
    var output: RDD[(String, String)] = sc.parallelize(List())
    val fs = FileSystem.get(new URI(Hdfs), new Configuration())

    // initial reading files
    output = sc.wholeTextFiles(dir, 3).coalesce(partition, false)
    numf = output.count().toInt


      // log
      log.write_log("<---- Iteration " + counter + " ---->")
      log.start()
      log.write_log("Number of Input Files : " + numf)
      println("<---- Iteration " + counter + " ---->")
      println("Number of Input Files : " + numf + "\n")

      // extract
      println("Extracting Trajectory Info...\n")
      val trajectories = ext.get_info_rdd(output)

      // get combination info
      val relations = rougen(trajectories)

      // create new gpx files
      numf = gpx(relations)

      // log

      log.write_log("Number of Output Files : " + numf)
      println("Number of Output Files : " + numf + "\n")
      log.finish()
    

  } // end of main
}
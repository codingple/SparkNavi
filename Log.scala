

/**
 * @author KwangMin
 */
import java.text.SimpleDateFormat
import java.util.Calendar
import Driver.logd
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.io.File
import java.net.URI
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._

object Log extends Serializable {
  val format = new SimpleDateFormat("yyyy_MM_dd_HH:mm:ss")
  var switch1 = false
  var switch2 = false
  var switch3 = false
  var switch4 = false
  var switch5 = false
  var switch6 = false
  var content: String = ""
  var counter: Int = 0
  var filename = "log" + counter

  def start() {
    content = content.concat("Strated at " + format.format(Calendar.getInstance().getTime()) + "\n")
    println("Strated at " + format.format(Calendar.getInstance().getTime()) + "\n")
    filename = "log" + counter
  }

  def check(): String = {
    return format.format(Calendar.getInstance().getTime())
  }

  def finish() {
    content = content.concat("Finished at " + format.format(Calendar.getInstance().getTime()) + "\n")
    println("Finished at " + format.format(Calendar.getInstance().getTime()) + "\n")
    val fs = FileSystem.get(new URI("hdfs://avocado:9000"), new Configuration())
    val pt = new Path(logd + "/" + filename + ".txt")
    val br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)))
    br.write(content + "\n\n")
    br.close()
    counter += 1
  }

  def write_log(s: String) {
    content = content.concat(s + "\n")
  }

  def algorithm_error() {
    content = content.concat("Algorithm Error detected at" + this.check() + "\n")
  }

}
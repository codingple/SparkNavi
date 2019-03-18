

/**
 * @author KwangMin
 */

import scala.io.Source
import Driver.dir
import java.net.URI
import org.apache.hadoop.fs._;
import org.apache.hadoop.conf._;
import org.apache.hadoop.io._;
import org.apache.hadoop.mapred._;
import org.apache.hadoop.util._;
import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import Driver.Hdfs

object File_Reader {
  val log = Log

  // read files
  def file_list(dir: String): List[String] = {
    var result: List[String] = List()
    val fs = FileSystem.get(new URI(Hdfs), new Configuration())
    val status = fs.listStatus(new Path(dir))
    val Num = status.length
    val iter = status.toIterator
    while (iter.hasNext) {
      val pt = iter.next().getPath().toString()
      result = result :+ pt
    }

    return result
  }

  def file_read(dir: String): List[(String, String)] = {
    var result: List[(String, String)] = List()
    val fs = FileSystem.get(new URI(Hdfs), new Configuration())
    val status = fs.listStatus(new Path(dir))
    val all = status.length
    val iter = status.toIterator
    var num = 0
    while (iter.hasNext) {
      var outline: String = ""
      val pt = iter.next().getPath().toString()
      val br = new BufferedReader(new InputStreamReader(fs.open(new Path(pt))))
      var line = br.readLine()
      while (line != null)
        outline = outline.concat(line)

      result = result :+ (pt, outline)
      println("GPX file created " + num + "/" + all + " : " + log.check())
      num += 1
    }

    return result
  }
}
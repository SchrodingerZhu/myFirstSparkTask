import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession

object Utils {


  val sparkConf = new SparkConf()
    .setMaster("spark://master:7077")
    .setAppName("Wiki")
    .set("spark.ui.enabled", "true")
  val session = SparkSession.builder.config(sparkConf).appName("Wiki").getOrCreate()
  val hadoopConf = new Configuration()
  hadoopConf.set("fs.defaultFS", "hdfs://master:9000")
  val fs =  FileSystem.get(hadoopConf)

  def openStream(s : String) : FSDataInputStream = {
    fs.open(new Path(s))
  }
}
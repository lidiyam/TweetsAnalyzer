import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TweetsAnalyzer {
  def main(args: Array[String]) {
    val logFile = "/Users/lidiyam/Developer/untitled/README.md"
    val conf = new SparkConf().setAppName("Tweets Analyzer")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }

}

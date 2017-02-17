import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

object TweetsAnalyzer {
  case class Trip(departure: String, arrival: String)

  def main(args: Array[String]) {
    val logFile = "/Users/lidiyam/Developer/tweets-analyzer/TweetsAnalyzer/README.md"
    val conf = new SparkConf().setAppName("Tweets Analyzer")
    conf.set("es.nodes", "localhost")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")


    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    EsSpark.saveToEs(rdd, "spark/docs")

    //sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

    sc.stop()
  }

}

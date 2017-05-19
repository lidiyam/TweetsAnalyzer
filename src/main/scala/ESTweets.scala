import TweetsAnalyzer.Record
import org.apache.spark.SparkConf
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object ESTweets {
  case class TweetRecord(id: Double, text: String, prediction: Double)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ES Tweets")
    conf.set("es.nodes", "localhost")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")

    val spark = SparkSession
      .builder()
      .appName("ES Tweets")
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val lrModel = PipelineModel.load("myModelPath")

    // get tweets from Elasticsearch
    val tweetsRDD = sc.esRDD("twitter_data/tweets").cache()

    val docs = tweetsRDD.map{ case (id, doc) => doc }
    println(s"# of docs: ${docs.count()}")
    val tweets = docs.map{
      case field => {
        val tweet_id = field.getOrElse("id", 0L).asInstanceOf[Long]
        val text = field.getOrElse("text", "").asInstanceOf[String]
        Record(tweet_id, text)
      }
    }.toDF()

    tweets.describe()

    val results = getTweetsSentimentScore(tweets,lrModel).select("label", "text", "prediction")
      .map{
        case row: Row => TweetRecord(row.getDouble(0), row.getString(1), row.getDouble(2))
      }

    results.saveToEs("es_tweets/sentiment")

  }

  def getTweetsSentimentScore(textDF: DataFrame, lrModel: PipelineModel): DataFrame = {
    lrModel.transform(textDF)
  }

}

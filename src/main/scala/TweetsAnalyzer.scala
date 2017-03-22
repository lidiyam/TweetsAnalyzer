import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.elasticsearch.spark._
import edu.stanford.nlp.simple._
import java.util._

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import training.LogisticRegressionTrain._

import scala.collection.JavaConversions._

object TweetsAnalyzer {
  case class Tweet(tweet_id: Long, text: String, language: String, timestamp: Date,
                   user_name: String, retweet_count: Long, hashtags: Array[String], sentiment: Double, positive: Boolean)
  case class TopHashtag(hashtag: String, count: Int, sentiment: Double)
  case class Record(label: Double, text: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Tweets Analyzer")
    conf.set("es.nodes", "localhost")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")

    val spark = SparkSession
      .builder()
      .appName("Tweets Analyzer")
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val data = sc.textFile("/Users/lidiyam/Developer/tweets-analyzer/TweetsAnalyzer/data/training-data.txt")
      .map(_.split("\t")).map {
      case Array(label, text) => Record(label.toDouble, text)
    }.toDF() // DataFrame will have columns "label" and "text"

    val lrPipeline = pipelineSetup(spark)
    val accuracy = evaluateModel(data, lrPipeline)

    println(s"Logistic Regression accuracy: $accuracy")

    // train model
    val lrModel = trainModel(data, lrPipeline)

    // get tweets from Elasticsearch
    val tweetsRDD = sc.esRDD("twitter_data/tweets").cache()
    println(s"# of tweets: ${tweetsRDD.count()}")

    val docs = tweetsRDD.map{ case (id, doc) => doc }
    val tweets = docs.map {
      case field => {
        val tweet_id = field.getOrElse("id", 0L).asInstanceOf[Long]
        val text = field.getOrElse("text", "").asInstanceOf[String]
        val lang = field.getOrElse("lang", "").asInstanceOf[String]
        val timestamp = field.getOrElse("@timestamp", "").asInstanceOf[Date]
        val username = field.getOrElse("user.screen_name", "").asInstanceOf[String]
        val retweet_count = field.getOrElse("retweet_count", 0L).asInstanceOf[Long]

        val hashtags = getHashtags(text)
        val sentiment = getSentiment(text)
        val positive = getSentimentLR(sc.makeRDD(Seq(Record(-1.0D, text))).toDF, lrModel)

        Tweet(tweet_id, text, lang, timestamp, username, retweet_count, hashtags, sentiment, positive)
      }
    }

    tweets.saveToEs("processed_tweets/tweet")

    val topHashTags = getTopHashtags(tweets, 25).toSeq
    sc.makeRDD(topHashTags).saveToEs("top_hashtags/hashtag")

    sc.stop()
  }

  def getHashtags(text: String): Array[String] = {
    text.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
  }

  def getSentiment(text: String): Double = {
    val sentences = new Document(text).sentences()
    val count = sentences.size()
    val sentiments = sentences.map( sentence => {
      sentence.sentiment() match {
        case SentimentClass.VERY_POSITIVE => 5
        case SentimentClass.POSITIVE => 4
        case SentimentClass.NEUTRAL => 3
        case SentimentClass.NEGATIVE => 2
        case SentimentClass.VERY_NEGATIVE => 1
      }
    } )
    sentiments.sum / count
  }

  def getTopHashtags(tweets: RDD[Tweet], size: Int): Array[TopHashtag] = {
    val topHashtags = tweets.flatMap(tweet => tweet.hashtags)
      .map(tag => (tag, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(size)

    topHashtags.map {
      case (tag, count) => TopHashtag(tag, count, getSentiment(tag))
    }
  }

  def getSentimentLR(textDF: DataFrame, lrModel: PipelineModel): Boolean = {
    lrModel.transform(textDF).select("text", "prediction").collect()
      .map{
        case Row(text: String, prediction: Double) => {
          println(s"tweet: $text")
          println(s"prediction: $prediction")
          (prediction == 1.0)
        }
      }.head
  }
}

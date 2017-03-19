import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.elasticsearch.spark._
import edu.stanford.nlp.simple._
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import java.util.Properties
import java.util._
import scala.collection.JavaConversions._

object TweetsAnalyzer {
  case class Tweet(tweet_id: Long, text: String, language: String, timestamp: Date,
                   user_name: String, retweet_count: Long, hashtags: Array[String], sentiment: Double)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Tweets Analyzer")
    conf.set("es.nodes", "localhost")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    val tweetsRDD = sc.esRDD("twitter_data/tweets")

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

        Tweet(tweet_id, text, lang, timestamp, username, retweet_count, hashtags, sentiment)
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

  def getTopHashtags(tweets: RDD[Tweet], count: Int): Array[(String, Double)] = {
    val hashtags = tweets.flatMap(tweet => tweet.hashtags)
    val topHashtags = hashtags
      .map(tag => (tag, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(count)
      .map(_._1)

    topHashtags.map {
      case tag => (tag, getSentiment(tag))
    }
  }

}

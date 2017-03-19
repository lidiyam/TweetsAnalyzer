package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object LogisticRegressionTrain {
  case class TweetSentiment(text: String, isHappy: Boolean, isSad: Boolean)

  val spark = SparkSession
    .builder()
    .appName("Spark SQL tweets training")
    .getOrCreate()

  // The schema is encoded in a string
  val schemaString = "text isHappy isSad"

  // Generate the schema based on the string of schema
  val fields = schemaString.split(" ")
    .map(fieldName => StructField(fieldName, StringType, nullable = true))
  val schema = StructType(fields)

  val data = spark.table("tweetData")
  //data: org.apache.spark.sql.DataFrame = [text: string, isHappy: boolean, isSad: boolean]

  val trainingData = data.withColumn("label", when(data("isHappy"), 1.0D).otherwise(0.0D))

  val tokenizer = new RegexTokenizer()
    .setGaps(false)
    .setPattern("\\p{L}+")
    .setInputCol("text")
    .setOutputCol("words")

  val stopwords: Array[String] = spark.sparkContext.textFile("/mnt/hossein/text/stopwords.txt").flatMap(_.stripMargin.split("\\s+")).collect ++ Array("rt")

  val filterer = new StopWordsRemover()
    .setStopWords(stopwords)
    .setCaseSensitive(false)
    .setInputCol("words")
    .setOutputCol("filtered")

  val countVectorizer = new CountVectorizer()
    .setInputCol("filtered")
    .setOutputCol("features")

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.2)
    .setElasticNetParam(0.0)

  val pipeline = new Pipeline().setStages(Array(tokenizer, filterer, countVectorizer, lr))

  val lrModel = pipeline.fit(trainingData)

  // todo: cross validation

}

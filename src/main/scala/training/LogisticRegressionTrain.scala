package training

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}

case class Record(label: Double, text: String)

object LogisticRegressionTrain {

  def process(spark: SparkSession): PipelineModel = {

    val sc = spark.sparkContext

    val data = sc.textFile("/Users/lidiyam/Developer/tweets-analyzer/TweetsAnalyzer/data/training-data.txt")

    import spark.implicits._

    val trainingData = data.map(_.split("\t")).map {
      case Array(label, text) => Record(label.toDouble, text)
    }.toDF() // DataFrame will have columns "label" and "text"

    trainingData.printSchema()

    val tokenizer = new RegexTokenizer()
      .setGaps(false)
      .setPattern("\\p{L}+")
      .setInputCol("text")
      .setOutputCol("words")

    val stopwords: Array[String] = spark.sparkContext.textFile("/Users/lidiyam/Developer/tweets-analyzer/TweetsAnalyzer/data/stopwords.txt")
      .flatMap(_.stripMargin.split("\\s+")).collect ++ Array("rt")

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

    pipeline.fit(trainingData)

  }

  def crossValidation(lrModel: PipelineModel, testDF: DataFrame) = {
    //    todo: cross validation
    //    val testDF = sc.makeRDD(Seq(Record(-1.0D, "I hate it"))).toDF
    lrModel.transform(testDF)
  }

}

package training

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}

case class Record(label: Double, text: String)

object LogisticRegressionTrain {

  def pipelineSetup(spark: SparkSession): Pipeline = {

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

    pipeline
  }

  def trainModel(trainingData: DataFrame, pipeline: Pipeline): PipelineModel = {
    pipeline.fit(trainingData)
  }

  def evaluateModel(data: DataFrame, pipeline: Pipeline): Double = {
    val Array(trainingData, testData) = data.randomSplit(Array(0.9, 0.1), seed = 12345)
    val model = trainModel(trainingData, pipeline)

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    val predictions = model.transform(testData)
    evaluator.evaluate(predictions)
  }

}

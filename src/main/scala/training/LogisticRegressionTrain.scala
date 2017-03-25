package training

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
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

  def pipelineTFIDF(): Pipeline = {

    val tokenizer = new RegexTokenizer()
      .setGaps(false)
      .setPattern("\\p{L}+")
      .setInputCol("text")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.2)
      .setElasticNetParam(0.0)

    val pipeline = new Pipeline().setStages(Array(tokenizer,hashingTF,idf,lr))

    pipeline
  }

  def crossValidationTF(pipeline: Pipeline, hashingTF: HashingTF, training: DataFrame): CrossValidatorModel = {
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(training)

    cvModel
  }

  def trainLRModel(spark: SparkSession, data: DataFrame): PipelineModel = {
    val lrPipelineCV = pipelineSetup(spark)
    val lrPipelineTF = pipelineTFIDF()
    val accuracyCV = evaluateModel(data, lrPipelineCV)
    val accuracyTF = evaluateModel(data, lrPipelineTF)

    println(s"Logistic Regression with CountVectorizer accuracy: $accuracyCV")
    println(s"Logistic Regression with HashingTF accuracy: $accuracyTF")

    val lrPipeline = if (accuracyCV >= accuracyTF) lrPipelineCV else lrPipelineTF

    // train model
    val lrModel = trainModel(data, lrPipeline)
    lrModel
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

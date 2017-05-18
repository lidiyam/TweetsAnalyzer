import TweetsAnalyzer.Record
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import training.LogisticRegressionTrain._

object TrainModel {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Train Model")

    val spark = SparkSession
      .builder()
      .appName("Tweets Training")
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val data = sc.textFile("/opt/training-data.txt")
      .map(_.split("\t")).map {
      case Array(label, text) => Record(label.toDouble, text)
    }.toDF() // DataFrame will have columns "label" and "text"

    val lrModel = trainLRModel(spark, data)

    lrModel.save("myModelPath")
  }

}

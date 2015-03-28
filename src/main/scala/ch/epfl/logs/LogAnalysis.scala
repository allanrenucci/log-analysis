package ch.epfl.logs

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by renucci on 20/03/15.
 */
object LogAnalysis {

//  val INPUT_SOURCE = "/user/renucci/input/clusterlogs"
  val INPUT_SOURCE = "/datasets//datasets/clusterlogs_hw2/*"
  val CROSS_VALIDATION_NUMBER = 5

  val FEATURES_NUMBER = 5

  object Status extends Enumeration {
    type Status = Value
    val UNSET, SUCCEEDED, FAILED = Value
  }

  import Status._

  case class Point(var status: Status, features: Array[Double]) {
    override def toString = {
      val fs = features mkString ("Array(", " ,", ")")
      s"Point($status, $fs)"
    }
  }

  private def getLabeledPoint(logs: Iterable[ApplicationLog]): Option[LabeledPoint] = {
    val point = Point(UNSET, Array.ofDim(FEATURES_NUMBER))

    logs foreach {
      case AppSummary(_, _, _, _, _, _, _, start, end, status) =>
        point.status = if (status == "FAILED") FAILED else SUCCEEDED
        point.features(0) = end - start

      case AllocatedContainer(_, _, _) =>
        point.features(1) += 1

      case KilledContainer(_, _, _) =>
        point.features(2) += 1

//      // Seems to be redundant
//      case ExitedWithSuccessContainer( _, _, _) =>
//        point.features(2) += 1

      case MemoryUsage(_, _, _, pMem, vMem) =>
        point.features(3) = point.features(3) max pMem
        point.features(4) = point.features(4) max vMem
    }

    val Point(status, features) = point
    //println(point)
    status match {
      case SUCCEEDED => Some(LabeledPoint(1.0, new DenseVector(features)))
      case FAILED    => Some(LabeledPoint(0.0, new DenseVector(features)))
      case UNSET     => None
    }
  }

  private def parseLine(line: String): Log =
    LogParser.parse(LogParser.logLine, line) getOrElse Undefined

  private def validateModel(data: RDD[LabeledPoint]): Double = {
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4))
    val training = splits(0)
    val test = splits(1)

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Compute raw scores on the test set
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    metrics.areaUnderROC()
  }

  def main(args: Array[String]) {
//    val sc = new SparkContext(new SparkConf() setAppName "LogAnalysis" setMaster "local")
    val sc = new SparkContext(new SparkConf() setAppName "LogAnalysis")
    val lines = sc textFile INPUT_SOURCE

    // Only keep useful log lines
    val logs = lines map parseLine collect { case log: ApplicationLog => log }

    // Logs grouped by application
    val appLogs = logs groupBy (_.appId)
    val data = (appLogs flatMap (l => getLabeledPoint(l._2))).cache()

    val meanROC = (0 until CROSS_VALIDATION_NUMBER)
      .map (_ => validateModel(data))
      .sum / CROSS_VALIDATION_NUMBER

    println("Area under ROC = " + meanROC)

    sc.stop()
  }

}

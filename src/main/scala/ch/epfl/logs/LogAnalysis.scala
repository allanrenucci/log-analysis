package ch.epfl.logs

import org.apache.spark.SparkContext.rddToPairRDDFunctions
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
  val INPUT_SOURCE = "/datasets/clusterlogs_hw2/*"
  val CROSS_VALIDATION_NUMBER = 5

  object Feature extends Enumeration {
    type Feature = Value
    type Features = Map[Feature, Double]
    val SUCCESS, TIME, ALLOC, KILLED, PMEM, VMEM = Value
    val orderedValues = (values - SUCCESS).toList
  }

  import Feature._


  private def getPoint(log: ApplicationLog): Features = log match {
    case AppSummary(_, _, _, _, _, _, _, start, end, status) =>
      val success = if (status == "FAILED") 0.0 else 1.0
      val time = end - start
      Map(SUCCESS -> success, TIME -> time)

    case AllocatedContainer(_, _, _) =>
      Map(ALLOC -> 1)

    case KilledContainer(_, _, _) =>
      Map(KILLED -> 1)

    case MemoryUsage(_, _, _, pMem, vMem) =>
      Map(PMEM -> pMem, VMEM -> vMem)
  }

  private def mergeFeatures(fs1: Features, fs2: Features): Features = (fs2 foldLeft fs1) {
    case (acc, succ @ (SUCCESS, _)) =>
      acc + succ

    case (acc, time @ (TIME, _)) =>
      acc + time

    case (acc, (ALLOC, killed)) =>
      val next = killed + acc.getOrElse(ALLOC, 0.0)
      acc + (ALLOC -> next)

    case (acc, (KILLED, killed)) =>
      val next = killed + acc.getOrElse(KILLED, 0.0)
      acc + (KILLED -> next)

    case (acc, (PMEM, pmem)) =>
      val next = pmem max acc.getOrElse(PMEM, 0.0)
      acc + (PMEM -> next)

    case (acc, (VMEM, vmem)) =>
      val next = vmem max acc.getOrElse(VMEM, 0.0)
      acc + (VMEM -> next)
  }

  private def getLabeledPoints(features: Features): Option[LabeledPoint] = {
    features get SUCCESS map { succ =>
      val fs = Feature.orderedValues map (f => features getOrElse (f, 0.0))
      LabeledPoint(succ, new DenseVector(fs.toArray))
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
    val maxBins = 30

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

    val logLines = sc textFile INPUT_SOURCE

    // Parse log lines and keep those associated to an application
    val logs = logLines map parseLine collect { case log: ApplicationLog => log }

    // Group logs by application id and get associated points
    val points = logs map(l => (l.appId, getPoint(l))) reduceByKey mergeFeatures

    // Get labeled points for MLlib
    val data = points flatMap (p => getLabeledPoints(p._2))

    data.cache()

    val meanROC = (0 until CROSS_VALIDATION_NUMBER)
      .map (_ => validateModel(data))
      .sum / CROSS_VALIDATION_NUMBER

    println("Area under ROC = " + meanROC)

    sc.stop()
  }

}

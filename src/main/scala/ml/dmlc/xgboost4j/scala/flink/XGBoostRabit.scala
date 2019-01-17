/*
This class was cloned from https://github.com/dmlc/xgboost/blob/master/jvm-packages/xgboost4j-flink/src/main/scala/ml/dmlc/xgboost4j/scala/flink/XGBoost.scala
The only difference is use of Scala RabitTracker class instead of the (Python)Java RabitTracker class
 */

package ml.dmlc.xgboost4j.scala.flink

import scala.collection.JavaConverters.asScalaIteratorConverter

import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.java.Rabit
import ml.dmlc.xgboost4j.scala.rabit.RabitTracker
import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost => XGBoostScala}

import org.apache.commons.logging.LogFactory
import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.util.Collector
import ml.dmlc.xgboost4j.scala.Booster

object XGBoostRabit {
  /**
    * Helper map function to start the job.
    *
    * @param workerEnvs
    */
  private class MapFunction(paramMap: Map[String, Any],
                            round: Int,
                            workerEnvs: java.util.Map[String, String])
    extends RichMapPartitionFunction[LabeledVector, Booster] {

    def mapPartition(it: java.lang.Iterable[LabeledVector],
                     collector: Collector[Booster]): Unit = {
      workerEnvs.put("DMLC_TASK_ID", String.valueOf(this.getRuntimeContext.getIndexOfThisSubtask))
      Rabit.init(workerEnvs)
      val mapper = (x: LabeledVector) => {
        val (index, value) = x.vector.toSeq.unzip
        LabeledPoint(x.label.toFloat, index.toArray, value.map(_.toFloat).toArray)
      }
      val dataIter = for (x <- it.iterator().asScala) yield mapper(x)
      val trainMat = new DMatrix(dataIter, null)
      val watches = List("train" -> trainMat).toMap
      val round = 2
      val numEarlyStoppingRounds = paramMap.get("numEarlyStoppingRounds")
        .map(_.toString.toInt).getOrElse(0)
      val booster = XGBoostScala.train(trainMat, paramMap, round, watches,
        earlyStoppingRound = numEarlyStoppingRounds)
      Rabit.shutdown()
      collector.collect(booster)
    }
  }



  /**
    * Train a xgboost model with Flink.
    *
    * @param dtrain The training data.
    * @param params The parameters to XGBoost.
    * @param round Number of rounds to train.
    */
  def train(dtrain: DataSet[LabeledVector], params: Map[String, Any], round: Int): Booster = {
    val tracker = new RabitTracker(dtrain.getExecutionEnvironment.getParallelism)
    if (tracker.start(30000L)) {
      dtrain
        .mapPartition(new MapFunction(params, round, tracker.getWorkerEnvs))
        .reduce((x, y) => x).collect().head
    } else {
      throw new Error("Tracker cannot be started")
      null
    }
  }
}

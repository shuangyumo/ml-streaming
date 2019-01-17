package fr.braux.test

import ml.dmlc.xgboost4j.scala.flink.XGBoostRabit
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.MLUtils

object StreamTrain extends App with Logging {
  if (args.length != 1) {
    System.err.println("USAGE: XGBoostTrain <FILE>")
    System.exit(1)
  }
  logger.info("training")
  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  val trainfile = args(0)
  val modelfile = trainfile.replace(".data", ".model")
  val traindata = MLUtils.readLibSVM(env, trainfile)
  val paramMap = List(
    "eta" -> 1,
    "max_depth" -> 2,
    "objective" -> "binary:logistic").toMap
  val round = 2
  val model = XGBoostRabit.train(traindata, paramMap, round)
  model.saveModel(modelfile)
}

// train the model and save it to file
import ml.dmlc.xgboost4j.scala.{XGBoost, DMatrix}

val dtrain = new DMatrix("data/agaricus.txt.train")
val params = List(
  "eta" -> 1.0,
  "max_depth" -> 2,
  "silent" -> 1,
  "objective" -> "binary:logistic").toMap
val booster = XGBoost.train(dtrain, params, 3)
booster.saveModel("src/test/resources/agaricus.model")

// run the model
val dtest = new DMatrix("data/agaricus.txt.test")
val predicts = booster.predict(dtest)

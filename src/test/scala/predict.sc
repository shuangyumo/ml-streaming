// predict from a single line and test svmToDMatrix

import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import ml.dmlc.xgboost4j.java.{DMatrix => JDMatrix}

val testrow = "0 1:1 9:1 19:1 21:1 24:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 122:1"
val shape = 127
val items = testrow.split(' ')
val label = items.head.toFloat
val (indices, values) = items.tail.map { item => (item.split(':')(0).toInt, item.split(':')(1).toFloat) }.unzip
val dtest = new DMatrix(List[Long](0, indices.length).toArray, indices, values, JDMatrix.SparseType.CSR, shape)
dtest.setLabel(List(label).toArray)

val model =  XGBoost.loadModel("src/test/resources/agaricus.model")
val ytest = model.predict(dtest)
// expecting ((0.15353635))




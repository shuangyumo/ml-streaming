package fr.braux.test

import ml.dmlc.xgboost4j.scala.DMatrix
import ml.dmlc.xgboost4j.java.{DMatrix => JDMatrix}

object Utils {

  // create DMatrix (CSR sparse matrix) from a SVM row; the shape (number of features) shall be specified as it cannot be guessed from a single row
  def svm2DMatrix(svmrow: String, shape: Int): DMatrix = {
    val items = svmrow.split(' ')
    val label = items.head.toFloat
    val (indices,values) = items.tail.map{item => (item.split(':')(0).toInt, item.split(':')(1).toFloat)}.unzip
    val dmat = new DMatrix(List[Long](0, indices.length).toArray, indices, values, JDMatrix.SparseType.CSR, shape)
    dmat.setLabel(List(label).toArray)
    dmat
  }

}

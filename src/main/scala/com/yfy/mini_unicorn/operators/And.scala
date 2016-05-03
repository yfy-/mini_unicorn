package com.yfy.mini_unicorn.operators

import com.yfy.mini_unicorn._
import com.yfy.mini_unicorn.helpers._
import org.apache.spark.rdd.RDD

/**
  * Created by yfy on 4/30/16.
  */
class And(parameters: Array[Operator], count: Int = 0, weight: Double = 0.0) extends
  Operator(count, weight) with OperatorHelper{

  override def execute(): Result = {
    val results = parameters.map(_.execute())

    if (weight == 0.0) {
      val intermediate = results.reduceLeft { (x, y) =>
        validate(x, y)
        CountResult(and(x.rdd, y.rdd), x.vertexType, 0)
      }

      return CountResult(intermediate.rdd, intermediate.vertexType, count)
    }

    if (count == 0.0) {
      val intermediate = results.reduceLeft { (x, y) =>
        validate(x, y)
        WeightResult(and(x.rdd, y.rdd), x.vertexType, 0.0)
      }

      return WeightResult(intermediate.rdd, intermediate.vertexType, weight)
    }

    operatorWithCountAndWeight
  }

  private def and(first: RDD[List[Hit]], second: RDD[List[Hit]]): RDD[List[Hit]] = {
    val cogrouped = cogroupRdds(first, second)

    val desired: RDD[(Null, List[Hit])] = cogrouped.map {
      case (null, (first: List[Hit], second: List[Hit])) =>
        (null, ListManipulator.intersect(first, second))
    }.mapValues(_.take(Config.truncationLimit))

    findDesiredInBoth(first, second, desired)
  }

  private def validate(first: Result, second: Result): Unit = {
    if (first.vertexType != second.vertexType) throw
      new Exception("Incompatible types " + first.vertexType + ", " + second.vertexType)

    if (first.optCount > 0 || second.optCount > 0) throw
      new Exception("and operator does not take optional count or weight")
  }
}

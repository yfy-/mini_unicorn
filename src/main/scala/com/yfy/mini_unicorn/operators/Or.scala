package com.yfy.mini_unicorn.operators
import com.yfy.mini_unicorn.helpers.{ListManipulator, OperatorHelper}
import com.yfy.mini_unicorn._
import org.apache.spark.rdd.RDD

/**
  * Created by yfy on 5/1/16.
  */
class Or(parameters: Array[Operator], count: Int = 0, weight: Double = 0.0) extends
  Operator(count, weight) with OperatorHelper{

  override def execute(): Result = {
    val results = parameters.map(_.execute())

    if (weight == 0.0) {
      val intermediate = results.reduceLeft { (x, y) =>
        validate(x, y)
        CountResult(or(x.rdd, y.rdd), x.vertexType, 0)
      }

      return CountResult(intermediate.rdd, intermediate.vertexType, count)
    }

    if (count == 0.0) {
      val intermediate = results.reduceLeft { (x, y) =>
        validate(x, y)
        WeightResult(or(x.rdd, y.rdd), x.vertexType, 0.0)
      }

      return WeightResult(intermediate.rdd, intermediate.vertexType, weight)
    }

    operatorWithCountAndWeight
  }

  private def or(first: RDD[List[Hit]], second: RDD[List[Hit]]): RDD[List[Hit]] = {
    val cogrouped = cogroupRdds(first, second)

    val desired: RDD[(Null, List[Hit])] = cogrouped.map {
      case (null, (first: List[Hit], second: List[Hit])) =>
        (null, ListManipulator.mergeSorted(first, second))
    }.mapValues(_.distinct.take(Config.truncationLimit))

    findDesiredInBoth(first, second, desired)
  }

  private def validate(first: Result, second: Result): Unit = {
    if (first.vertexType != second.vertexType) throw
      new Exception("Incompatible types " + first.vertexType + ", " + second.vertexType)

    if (first.optCount > 0 || second.optCount > 0) throw
      new Exception("or operator cannot have an optional count or weight")
  }
}

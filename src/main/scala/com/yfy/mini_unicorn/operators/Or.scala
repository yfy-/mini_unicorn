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
    val first = parameters(0).execute()
    val second = parameters(1).execute()

    if (weight == 0.0) return CountResult(or(first, second), first.vertexType, count)
    if (count == 0) return WeightResult(or(first, second), first.vertexType, weight)

    operatorWithCountAndWeight
  }

  private def or(first: Result, second: Result): RDD[List[Hit]] = {
    if (first.vertexType != second.vertexType) throw
      new Exception("Incompatible types " + first.vertexType + ", " + second.vertexType)

    if (first.optCount > 0 || second.optCount > 0) throw
      new Exception("or operator can not have an optional count or weight")

    val cogrouped = cogroupRdds(first.rdd, second.rdd)

    val desired: RDD[(Null, List[Hit])] = cogrouped.map {
      case (null, (first: List[Hit], second: List[Hit])) =>
        (null, ListManipulator.mergeSorted(first, second))
    }.mapValues(_.distinct.take(Config.truncationLimit))

    findDesiredInBoth(first.rdd, second.rdd, desired)
  }
}

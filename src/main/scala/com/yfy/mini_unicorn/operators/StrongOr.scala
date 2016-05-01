package com.yfy.mini_unicorn.operators

import com.yfy.mini_unicorn._
import com.yfy.mini_unicorn.helpers.{ListManipulator, OperatorHelper}
import org.apache.spark.rdd.RDD

/**
  * Created by yfy on 5/1/16.
  */
class StrongOr(
      firstParam: Operator,
      secondParam: Operator,
      count: Int = 0,
      weight: Double = 0.0) extends Operator(count, weight) with OperatorHelper{

  override def execute(): Result = {
    val first = firstParam.execute()
    val second = secondParam.execute()

    val weightF = assertWeightResult(first)
    val weightS = assertWeightResult(second)

    if (weight == 0.0) return CountResult(strongOr(weightF, weightS), first.vertexType, count)
    if (count == 0) return WeightResult(strongOr(weightF, weightS), first.vertexType, weight)

    operatorWithCountAndWeight
  }

  private def strongOr(first: WeightResult, second: WeightResult): RDD[List[Hit]] = {

    if (first.vertexType != second.vertexType) throw
      new Exception("Incompatible types " + first.vertexType + ", " + second.vertexType)

    if (first.weight + second.weight > 1.0) throw
      new Exception("Sum of optional-weight's of strong-or operands can't be bigger than 1.0")

    val cogrouped = cogroupRdds(first.rdd, second.rdd).cache()

    val desired = cogrouped.map {
      case (null, (f: List[Hit], s: List[Hit])) =>
        (null, ListManipulator.strongOr(f, s, first.weight, second.weight))
    }

    findDesiredInBoth(first.rdd, second.rdd, desired)
  }

  private def assertWeightResult(result: Result): WeightResult = result match {
    case w: WeightResult => w
    case c: CountResult => throw new Exception("strong-or can only work with optional-weight.")
  }
}

package com.yfy.mini_unicorn.operators

import com.yfy.mini_unicorn._
import com.yfy.mini_unicorn.helpers._
import org.apache.spark.rdd.RDD

/**
  * Created by yfy on 4/30/16.
  */
class AndOperator(params: Array[Parameterizable]) extends Operator with AndHelper{

  override val parameters: Array[Parameterizable] = params

  override def execute(count: Int): CountResult = {
    val first = retrieveParameter(parameters(0))
    val second = retrieveParameter(parameters(1))

    CountResult(executeInternal(first, second), first.vertexType, count)
  }

  override def execute(weight: Double): WeightResult = {
    val first = retrieveParameter(parameters(0))
    val second = retrieveParameter(parameters(1))

    WeightResult(executeInternal(first, second), first.vertexType, weight)
  }

  private def retrieveParameter(p: Parameterizable): Result = p match {
    case r: Result => r
    case _ => throw new Exception("Invalid Parameter")
  }

  private def executeInternal(first: Result, second: Result): RDD[List[Hit]] = {
    if (first.vertexType != second.vertexType) throw
      new Exception("Incompatible types " + first.vertexType + ", " + second.vertexType)

    if (first.optCount > 0 || second.optCount > 0) throw
      new Exception("and operator does not take optional count or weight")

    val cogrouped = cogroupRdds(first.rdd, second.rdd)

    val fromBoth: RDD[(Null, List[Hit])] = cogrouped.map {
      case (null, (first: List[Hit], second: List[Hit])) =>
        (null, ListManipulator.intersect(first, second))
    }

    val desired = fromBoth.reduceByKey( (x, y) =>
      ListManipulator.mergeSorted(x, y), Config.numPartitions).mapValues(_.take(Config.truncationLimit))

    findDesiredInBoth(first.rdd, second.rdd, desired)
  }
}

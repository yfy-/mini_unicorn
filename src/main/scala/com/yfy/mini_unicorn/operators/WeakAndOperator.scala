package com.yfy.mini_unicorn.operators

import org.apache.spark.rdd.RDD
import com.yfy.mini_unicorn._
import com.yfy.mini_unicorn.helpers._

/**
  * Created by yfy on 5/1/16.
  */
class WeakAndOperator(params: Array[Parameterizable]) extends Operator with AndHelper{

  override val parameters: Array[Parameterizable] = params

  override def execute(count: Int): CountResult = {
    val first = retrieveParameter(parameters(0))
    val second = retrieveParameter(parameters(1))

    CountResult(weakAndInternal(first, second), first.vertexType, count)
  }

  override def execute(weight: Double): WeightResult = ???

  private def retrieveParameter(p: Parameterizable): Result = p match {
    case r: Result => r
    case _ => throw new Exception("Invalid Parameter")
  }

  private def weakAndInternal(first: Result, second: Result): RDD[List[Hit]] = {
    if (first.vertexType != second.vertexType) throw
      new Exception("Incompatible types " + first.vertexType + ", " + second.vertexType)

    val cogrouped = cogroupRdds(first.rdd, second.rdd).cache

    val fromBoth: RDD[(Null, List[Hit])] = cogrouped.map {
      case (null, (f: List[Hit], s: List[Hit])) =>
        (null, ListManipulator.intersect(f, s))
    }

    val fromFirst: RDD[(Null, List[Hit])] = cogrouped.map
    { case (null, (f: List[Hit], s: List[Hit])) =>
      (null, f.diff(s).take(second.optCount))
    }

    val fromSecond: RDD[(Null, List[Hit])] = cogrouped.map
    { case (null, (f: List[Hit], s: List[Hit])) =>
      (null, s.diff(f).take(first.optCount))
    }

    cogrouped.unpersist()

    val desired = (fromBoth ++ fromFirst ++ fromSecond).reduceByKey( (x, y) =>
      ListManipulator.mergeSorted(x, y), Config.numPartitions).mapValues(_.take(Config.truncationLimit))

    findDesiredInBoth(first.rdd, second.rdd, desired)
  }
}

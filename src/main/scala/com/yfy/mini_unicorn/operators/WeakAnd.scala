package com.yfy.mini_unicorn.operators

import org.apache.spark.rdd.RDD
import com.yfy.mini_unicorn._
import com.yfy.mini_unicorn.helpers._

/**
  * Created by yfy on 5/1/16.
  */
class WeakAnd(
       firstParam: Operator,
       secondParam: Operator,
       count: Int = 0,
       weight: Double = 0.0) extends Operator(count, weight) with OperatorHelper{

  override def execute(): Result = {
    val first = firstParam.execute()
    val second = secondParam.execute()

    if (weight == 0.0) return CountResult(weakAnd(first, second), first.vertexType, count)
    if (count == 0) return WeightResult(weakAnd(first, second), first.vertexType, weight)

    operatorWithCountAndWeight
  }

  private def weakAnd(first: Result, second: Result): RDD[List[Hit]] = {
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

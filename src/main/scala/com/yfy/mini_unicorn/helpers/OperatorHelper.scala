package com.yfy.mini_unicorn.helpers

import com.yfy.mini_unicorn.partitioners.RandomPartitioner
import com.yfy.mini_unicorn._
import org.apache.spark.rdd.RDD

/**
  * Created by yfy on 5/1/16.
  */
trait OperatorHelper {

  protected def cogroupRdds(
                 firstRdd: RDD[List[Hit]],
                 secondRdd: RDD[List[Hit]]): RDD[(Null, (List[Hit], List[Hit]))] = {

    firstRdd.map((null, _)).cogroup(secondRdd.map((null, _))).map {
      case (null, (first: Iterable[List[Hit]], second: Iterable[List[Hit]])) =>
        val firstMerged = first.reduceLeft((x, y) => ListManipulator.mergeSorted(x, y))
        val secondMerged = second.reduceLeft((x, y) => ListManipulator.mergeSorted(x, y))
        (null, (firstMerged, secondMerged))
    }
  }

  // Lists can contain duplicate elements, does not effect the result of queries.
  protected def findDesiredInBoth(
                                   firstRdd: RDD[List[Hit]],
                                   secondRdd: RDD[List[Hit]],
                                   desired: RDD[(Null, List[Hit])]): RDD[List[Hit]] = {

    val combined = firstRdd.zipPartitions(secondRdd, preservesPartitioning = true)((f, s) =>
      ListManipulator.mergeIteratorsSorted(f, s)
    ).map((null, _))

    firstRdd.unpersist()
    secondRdd.unpersist()

    val joined = combined.join(desired, Config.numPartitions).partitionBy(
      new RandomPartitioner(Config.numPartitions)).map
    { case (null, (c: List[Hit], d: List[Hit])) =>
      ListManipulator.intersect(c, d)
    }.filter(_.nonEmpty)

    joined.mapPartitions {
      it =>
        if (it.nonEmpty) List(it.toList.reduceLeft((x, y) =>
          ListManipulator.mergeSorted(x, y)).distinct).iterator
        else it
    }
  }
}

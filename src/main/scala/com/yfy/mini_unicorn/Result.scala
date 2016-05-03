package com.yfy.mini_unicorn

import com.yfy.mini_unicorn.helpers.ListManipulator
import com.yfy.mini_unicorn.types.VertexType
import org.apache.spark.rdd.RDD

abstract class Result(rddData: RDD[List[Hit]], vType: VertexType) extends Serializable{
  val vertexType = vType
  val rdd = rddData.cache
  val optCount: Int

  def collect: Array[Hit] = keyWithNullAndMerge.mapValues(_.distinct).flatMap(_._2).collect

  private def keyWithNullAndMerge: RDD[(Null, List[Hit])] = {
    val result = rdd.map((null, _)).reduceByKey((x, y) =>
      ListManipulator.mergeSorted(x, y), Config.numPartitions)
    rdd.unpersist()
    result
  }
}

case class CountResult(
  rddData: RDD[List[Hit]],
  vType: VertexType,
  count: Int) extends Result(rddData, vType) {
  override val optCount: Int = count
}

case class WeightResult(
  rddData: RDD[List[Hit]],
  vType: VertexType,
  weight: Double) extends Result(rddData, vType) {
  override val optCount: Int = (rddData.map(_.size).reduce(_ + _) * weight).asInstanceOf[Int]
}

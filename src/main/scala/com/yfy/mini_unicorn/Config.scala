package com.yfy.mini_unicorn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Config {
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val numPartitions = 8
  val truncationLimit = 1000

  def printPartitions(rdd: RDD[_], first: Int = 100): Unit = {
    rdd.mapPartitionsWithIndex((index, it) =>
      it.toList.take(first).map(x => println(index + "->" + x)).iterator).collect
  }

  def stop() = sc.stop
}

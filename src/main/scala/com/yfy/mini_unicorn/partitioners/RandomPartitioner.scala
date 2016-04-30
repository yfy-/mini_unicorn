package com.yfy.mini_unicorn.partitioners

import org.apache.spark.Partitioner

import scala.util.Random

class RandomPartitioner(numParts: Int) extends Partitioner {
  override val numPartitions = numParts

  override def getPartition(key: Any): Int = {
    Random.nextInt(numParts)
  }

  override def equals(other: Any): Boolean = other match {
    case rd: RandomPartitioner =>
      rd.numPartitions == numPartitions
    case _ => false
  }
}

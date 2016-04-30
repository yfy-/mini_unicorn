package com.yfy.mini_unicorn.partitioners

import org.apache.spark.Partitioner

class SequentialPartitioner(numParts: Int) extends Partitioner {
  private var currentPartition: Int = 0

  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    currentPartition += 1
    (currentPartition - 1) % numPartitions
  }

  override def equals(other: Any): Boolean = other match {
    case sp: SequentialPartitioner =>
      sp.numPartitions == numPartitions
    case _ =>
      false
  }
}

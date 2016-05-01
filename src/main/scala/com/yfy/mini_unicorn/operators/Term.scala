package com.yfy.mini_unicorn.operators

import com.yfy.mini_unicorn._
import org.apache.spark.rdd.RDD

/**
  * Created by yfy on 4/30/16.
  */
class Term(parameter: EdgeIdPair, count: Int = 0, weight: Double = 0.0) extends Operator(count, weight) {

  override def execute(): Result = {
    val edgeIdPair = parameter

    if (weight == 0.0 ) return CountResult(term(edgeIdPair), edgeIdPair.eType.src, count)
    if (count == 0) return WeightResult(term(edgeIdPair), edgeIdPair.eType.src, weight)

    operatorWithCountAndWeight
  }

  private def term(term: EdgeIdPair): RDD[List[Hit]] = {
    SocialGraph.postingLists.filter { case (t, lh) => t == term}.values
  }
}

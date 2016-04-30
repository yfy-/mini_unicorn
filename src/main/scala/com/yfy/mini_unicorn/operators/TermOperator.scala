package com.yfy.mini_unicorn.operators

import com.yfy.mini_unicorn._
import org.apache.spark.rdd.RDD

/**
  * Created by yfy on 4/30/16.
  */
class TermOperator(params: Array[Parameterizable]) extends Operator{

  override val parameters: Array[Parameterizable] = params

  private def termInternal(term: Term): RDD[List[Hit]] = {
    SocialGraph.postingLists.filter { case (t, lh) => t == term}.values
  }

  override def execute(count: Int): CountResult = {
    val term: Term = parameters(0) match {
      case t: Term => t
      case _ => throw new Exception("Invalid parameter")
    }
    CountResult(termInternal(term), term.eType.src, count)
  }

  override def execute(weight: Double): WeightResult = {
    val term: Term = parameters(0) match {
      case t: Term => t
      case _ => throw new Exception("Invalid parameter")
    }

    WeightResult(termInternal(term), term.eType.src, weight)
  }
}

package com.yfy.mini_unicorn.operators
import com.yfy.mini_unicorn.types.EdgeType
import com.yfy.mini_unicorn.{CountResult, EdgeIdPair, Result, WeightResult}

/**
  * Created by yfy on 5/3/16.
  */
class Apply(
             firstParam: EdgeType,
             secondParam: Operator,
             count: Int = 0,
             weight: Double = 0.0) extends Operator(count, weight) {

  override def execute(): Result = {
    val terms: Array[Operator] = secondParam.execute().collect.map(x =>
      new Term(EdgeIdPair(firstParam, x.docId.id)))

    val applyResult = new Or(terms.take(100)).execute()

    if (weight == 0.0) return CountResult(applyResult.rdd, applyResult.vertexType, count)
    if (count == 0) return WeightResult(applyResult.rdd, applyResult.vertexType, weight)

    operatorWithCountAndWeight
  }
}

package com.yfy.mini_unicorn.operators

import com.yfy.mini_unicorn._

/**
  * Created by yfy on 4/30/16.
  */
trait Operator {
  def execute(count: Int): CountResult
  def execute(weight: Double): WeightResult
  val parameters: Array[Parameterizable]
}

package com.yfy.mini_unicorn.operators

import com.yfy.mini_unicorn._

/**
  * Created by yfy on 4/30/16.
  */
abstract class Operator(count: Int = 0, weight: Double = 0.0) extends Parameterizable{
  if (count != 0 && weight != 0) operatorWithCountAndWeight
  val optCount = count
  val optWeight = weight

  def execute(): Result

  protected def operatorWithCountAndWeight = throw
    new Exception("An operator can't take both count and weight as it's optional parameters.")
}

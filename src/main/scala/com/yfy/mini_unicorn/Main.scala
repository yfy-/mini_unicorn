package com.yfy.mini_unicorn

import com.yfy.mini_unicorn.operators._
import com.yfy.mini_unicorn.types.FriendEdge

object Main {
  def main(args: Array[String]): Unit = {

    val one = EdgeIdPair(FriendEdge, 1)
    val six = EdgeIdPair(FriendEdge, 6)

    val andResult = new And(Array(new Term(one), new Term(six))).execute().collect
    println("AND")
    andResult.foreach(println)

    val weakAndResult = new WeakAnd(new Term(one, weight = 0.2), new Term(six, count = 1)).
      execute().collect
    println("WEAK-AND")
    weakAndResult.foreach(println)

    val orResult = new Or(Array(new Term(one), new Term(six))).execute().collect
    println("OR")
    orResult.foreach(println)

    val strongOrResult = new StrongOr(new Term(one, weight = 0.1), new Term(six, weight = 0.6)).
      execute().collect
    println("STRONG-OR")
    strongOrResult.foreach(println)

    scala.io.StdIn.readLine

    Config.stop()
  }
}

package com.yfy.mini_unicorn

import com.yfy.mini_unicorn.operators._
import com.yfy.mini_unicorn.types.FriendEdge

object Main {
  def main(args: Array[String]): Unit = {

    val one = EdgeIdPair(FriendEdge, 1)
    val six = EdgeIdPair(FriendEdge, 6)
    val fortyone = EdgeIdPair(FriendEdge, 41)

    val andResult = new And(Array(new Term(one), new Term(six))).execute().collect
    println("AND")
    andResult.foreach(println)
    scala.io.StdIn.readLine

    // (weak-and (friend:5 opt-weight: 0.2) (friend:6 opt-count: 1))
    val weakAndResult = new WeakAnd(new Term(one, weight = 0.2), new Term(six, count = 1)).
      execute().collect
    println("WEAK-AND")
    weakAndResult.foreach(println)
    scala.io.StdIn.readLine

    val orResult = new Or(Array(new Term(one), new Term(six))).execute().collect
    println("OR")
    orResult.foreach(println)
    scala.io.StdIn.readLine

    val strongOrResult = new StrongOr(new Term(one, weight = 0.1), new Term(six, weight = 0.6)).
      execute().collect
    println("STRONG-OR")
    strongOrResult.foreach(println)
    scala.io.StdIn.readLine

    //(and (or friend:1 friend:6) (strong-or friend:1 friend:6))
    val complexResult = new And(Array(new Or(Array(new Term(one), new Term(six))),
      new StrongOr(new Term(one, weight = 0.1), new Term(six, weight = 0.6)))).execute().collect

    println("COMPLEX")
    complexResult.foreach(println)
    scala.io.StdIn.readLine

    val applyResult = new Apply(FriendEdge, new Term(EdgeIdPair(FriendEdge, 37))).execute().collect

    println("APPLY")
    applyResult.foreach(println)
    scala.io.StdIn.readLine

    Config.stop()
  }
}

package com.yfy.mini_unicorn

import com.yfy.mini_unicorn.operators.{AndOperator, TermOperator, WeakAndOperator}
import com.yfy.mini_unicorn.types.FriendEdge

object Main {
  def main(args: Array[String]): Unit = {

    val firstTermOp = new TermOperator(Array(Term(FriendEdge, 1)))
    val secondTermOp = new TermOperator(Array(Term(FriendEdge, 6)))

    val first = firstTermOp.execute(0)
    val second = secondTermOp.execute(0)

    val res = new AndOperator(Array(first, second)).execute(0)

    res.collect.foreach(println)

    val firstWeak = firstTermOp.execute(1)
    val secondWeak = secondTermOp.execute(1)

    val resWeak = new WeakAndOperator(Array(firstWeak, secondWeak)).execute(0)

    resWeak.collect.foreach(println)

    scala.io.StdIn.readLine

    Config.stop()
  }
}

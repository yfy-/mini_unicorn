package com.yfy.mini_unicorn.helpers

import com.yfy.mini_unicorn.{Config, Hit}
import scala.util.control.Breaks._

/**
  * Created by yfy on 5/1/16.
  */
object ListManipulator {

  def mergeSorted(x: List[Hit], y: List[Hit]): List[Hit] = {
    if (x.isEmpty) return y

    if (y.isEmpty) return x

    if (x.head.docId.rank >= y.head.docId.rank) {
      x.head :: mergeSorted(x.tail, y)
    } else {
      y.head :: mergeSorted(x, y.tail)
    }
  }

  def intersect(first: List[Hit], second: List[Hit]): List[Hit] = {
    var res = scala.collection.mutable.ListBuffer.empty[Hit]
    for (f <- first) {
      breakable {
        for (s <- second) {
          if (f == s) res += f
          if (f.docId.rank > s.docId.rank) break
        }
      }
    }

    res.toList
  }

  def strongOr(first: List[Hit], second: List[Hit], fWeight: Double, sWeight: Double): List[Hit] = {
    if ((first ++ second).distinct.size > Config.truncationLimit) {
      val fDefiniteCount = (Config.truncationLimit * fWeight).asInstanceOf[Int]
      val sDefiniteCount = (Config.truncationLimit * sWeight).asInstanceOf[Int]

      val firstSplitted = first.splitAt(fDefiniteCount)
      val secondSplitted = second.splitAt(sDefiniteCount)

      val definite = mergeSorted(firstSplitted._1, secondSplitted._1).distinct
      val maybe = mergeSorted(firstSplitted._2, secondSplitted._2).distinct
      val truncated = maybe.diff(definite).take(Config.truncationLimit - definite.size)

      mergeSorted(definite, truncated)

    } else
      mergeSorted(first, second).distinct
  }

  def mergeIteratorsSorted(
      first: Iterator[List[Hit]],
      second: Iterator[List[Hit]]): Iterator[List[Hit]] = {

    if (!first.hasNext) return second
    if (!second.hasNext) return first

    // Guaranteed that there is only a single list in each iterator
    Iterator[List[Hit]](mergeSorted(first.next, second.next).distinct)
  }
}

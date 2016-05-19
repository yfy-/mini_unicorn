package com.yfy.mini_unicorn.helpers

import com.yfy.mini_unicorn.{Config, Hit}

/**
  * Created by yfy on 5/1/16.
  */
object ListManipulator {

  def mergeSorted(l1: List[Hit], l2: List[Hit]): List[Hit] = {
    mergeSorted(l1, l2, List.empty[Hit])
  }

  private def mergeSorted(l1: List[Hit], l2: List[Hit], result: List[Hit]): List[Hit] = {
    if (l1.isEmpty) return result ++ l2

    if (l2.isEmpty) return result ++ l1

    if (l1.head.docId < l2.head.docId) {
      mergeSorted(l1.tail, l2, result :+ l1.head)
    } else {
      mergeSorted(l1, l2.tail, result :+ l2.head)
    }
  }

  def intersect(l1: List[Hit], l2: List[Hit]): List[Hit] = {
    intersect(l1, l2, List.empty[Hit])
  }

  private def intersect(l1: List[Hit], l2: List[Hit], result: List[Hit]): List[Hit] = {
    if (l1.isEmpty || l2.isEmpty)
      return result
    if (l1.head.docId == l2.head.docId)
      return intersect(l1.tail, l2.tail, result :+ l1.head)
    if (l1.head.docId < l2.head.docId)
      return intersect(l1.tail, l2, result)

    intersect(l1, l2.tail, result)
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

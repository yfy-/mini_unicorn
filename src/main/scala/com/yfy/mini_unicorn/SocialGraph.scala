package com.yfy.mini_unicorn

import com.yfy.mini_unicorn.types.EdgeType

object SocialGraph extends Serializable {
  val postingLists = GraphLoader.load("Slashdot0902").cache
}

case class EdgeIdPair(eType: EdgeType, id: Int) extends Serializable

case class Hit(docId: DocId, hitData: String) extends Serializable

case class DocId(id: Int, var rank: Double) extends Serializable {
  def <(that: DocId): Boolean = {
    if (this.rank == that.rank)
      return this.id < that.id

    this.rank > that.rank
  }

  def >(that: DocId): Boolean = !(this < that)
}

object DocIdOrdering extends Ordering[DocId] {
  def compare(x: DocId, y: DocId): Int = {
    if (x < y) return -1
    if (x == y) return 0
    1
  }
}

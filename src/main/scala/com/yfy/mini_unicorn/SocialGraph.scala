package com.yfy.mini_unicorn

import com.yfy.mini_unicorn.types.EdgeType

object SocialGraph extends Serializable {
  val postingLists = GraphLoader.load("Slashdot0902").cache
}

case class Term(eType: EdgeType, id: Int) extends Serializable with Parameterizable

case class DocId(id: Int, var rank: Double) extends Serializable

case class Hit(docId: DocId, hitData: String) extends Serializable

object HitOrdering extends Ordering[Hit] {
  def compare(x: Hit, y: Hit): Int = DocIdOrdering.compare(x.docId, y.docId)
}

object DocIdOrdering extends Ordering[DocId] {
  def compare(x: DocId, y: DocId): Int = x.rank compare y.rank
}

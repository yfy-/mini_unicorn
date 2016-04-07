import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext

class SocialGraph(adjRDD: RDD[(Term, List[Hit])]) extends Serializable {
  val postingLists = adjRDD
  postingLists.cache

  def term(term: Term) = {
    postingLists.filter { case (t, lh) => term == t }.flatMap(_._2).sortBy(
      _.docId.rank, false)
  }

  def and(rdds: List[RDD[Hit]]): RDD[Hit] = {
    rdds.reduceLeft((x, y) =>
    x.intersection(y,
      x.partitioner.getOrElse(
        new HashPartitioner(x.getNumPartitions)))(HitOrdering))
    }

  //not sorted
  def or(rdds: List[RDD[Hit]]): RDD[Hit] = {
    rdds.reduceLeft(_ ++ _).distinct
  }

  def mergeSorted(x: List[Hit], y: List[Hit]): List[Hit] = {
    if (x.isEmpty) return y

    if (y.isEmpty) return x

    if (x.head.docId.rank >= y.head.docId.rank) {
      return x.head :: mergeSorted(x.tail, y)
    } else {
      return y.head :: mergeSorted(x, y.tail)
    }
  }
}

case class Term(eType: EdgeType, id: Int) extends Serializable

case class DocId(vType: VertexType, id: Int, var rank: Double) extends Serializable

case class Hit(docId: DocId, hitData: String) extends Serializable

object HitOrdering extends Ordering[Hit] {
  def compare(x: Hit, y: Hit): Int = DocIdOrdering.compare(x.docId, y.docId)
}

object DocIdOrdering extends Ordering[DocId] {
  def compare(x: DocId, y: DocId): Int = x.rank compare y.rank
}

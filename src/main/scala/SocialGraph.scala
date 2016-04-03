import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class SocialGraph(adjRDD: RDD[(Term, List[Hit])]) extends Serializable {
  val postingLists = adjRDD
  postingLists.cache

  def term(term: Term): RDD[Hit] = {
    postingLists.filter { case (t, lh) => term == t }.reduceByKey( (x, y) =>
      x ++ y).flatMap(_._2).sortBy(_.docId.rank, false)
  }

  def and(rdds: List[RDD[Hit]]): RDD[Hit] = rdds.reduceLeft(_.intersection(_))

  def or(rdds: List[RDD[Hit]]): RDD[Hit] = rdds.reduceLeft(_.union(_)).distinct

//  def mergeSorted(x: List[Hit], y: List[Hit]): List[Hit] = {
//    if (x.isEmpty) return y
//
//    if (y.isEmpty) return x
//
//    if (x.head.docId.rank >= y.head.docId.rank) {
//      return x.head :: mergeSorted(x.tail, y)
//    } else {
//      return y.head :: mergeSorted(x, y.tail)
//    }
//  }
}

case class Term(eType: EdgeType, id: Int) extends Serializable
case class DocId(vType: VertexType, id: Int, var rank: Double) extends Serializable
case class Hit(docId: DocId, hitData: String) extends Serializable

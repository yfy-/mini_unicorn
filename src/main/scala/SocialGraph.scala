import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class SocialGraph(adjRDD: RDD[(Term, List[Hit])]) extends Serializable {
  val postingLists = adjRDD
  postingLists.cache()

  def term(term: Term): RDD[Hit] = {
    postingLists.filter { case (t, lh) => term == t }.reduceByKey( (x, y) =>
      x ++ y).flatMap(_._2)
  }
}

case class Term(eType: EdgeType, id: Int) extends Serializable
case class DocId(vType: VertexType, id: Int, var rank: Double) extends Serializable
case class Hit(docId: DocId, hitData: String) extends Serializable










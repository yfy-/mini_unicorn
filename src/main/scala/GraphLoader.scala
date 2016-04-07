import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, HashPartitioner}

object GraphLoader extends Serializable {
  private val adjSpec = "-adj"
  private val rankSpec = "-rank"
  private val fileExt = ".csv"

  def load(sc: SparkContext, data: String, numPartitions: Int = 8): SocialGraph = {
    val rankRdd = loadRanks(sc, data)

    val adjLines = sc.textFile("./src/main/resources/" + data + "/" + data + adjSpec + fileExt)

    val adjPairs = adjLines.map { al =>
      val sp = al.split(";")
      (sp(1).toInt, sp(0).toInt)
    }.partitionBy(new HashPartitioner(numPartitions)).join(rankRdd).map { case (adj, (node, rank)) =>
        (Term(FriendEdge, node), List(Hit(DocId(PersonVertex, adj, rank), "")))
    }.partitionBy(new RandomPartitioner(numPartitions))

    val mergedPairs = adjPairs.mapPartitions(it =>
      it.toList.groupBy(x => x._1). map { case (k, v) =>
        (k, v.flatMap(_._2).sortBy(_.docId.rank)(Ordering[Double].reverse)) }.iterator,
      true)

    new SocialGraph(mergedPairs)
  }

  private def loadRanks(sc: SparkContext, data: String): RDD[(Int, Double)] = {
    val rankLines = sc.textFile("./src/main/resources/" + data + "/" + data + rankSpec + fileExt)
    val rankRdd = rankLines.map { rl =>
      val sp = rl.split(";")
      (sp(0).toInt, sp(1).toDouble)
    }

    rankRdd
  }
}

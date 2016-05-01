package com.yfy.mini_unicorn

import com.yfy.mini_unicorn.partitioners.RandomPartitioner
import com.yfy.mini_unicorn.types.{EdgeType, FriendEdge, PersonVertex, VertexType}
import org.apache.spark.graphx.{Edge, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}

/**
  * Created by yfy on 5/1/16.
  */
object GraphLoader extends Serializable {
  private val adjSpec = "-adj"
  private val rankSpec = "-rank"
  private val fileExt = ".csv"
  private val yfyHome = "/home/yfy/scala-workspace/mini_unicorn"

  def load(data: String): RDD[(EdgeIdPair, List[Hit])] = {
    val sc = Config.sc
    val numPartitions = Config.numPartitions

    val rankRdd = loadRanks(sc, data)
    val adjRdd = loadAdjacents(sc, data)

    val adjPairs = adjRdd.map { case (src, dest) => (dest, src) }.partitionBy(
      new HashPartitioner(numPartitions)).join(rankRdd).map
    { case (adj, (node, rank)) =>
        (EdgeIdPair(FriendEdge, node), List(Hit(DocId(adj, rank), "")))
    }.partitionBy(new RandomPartitioner(numPartitions))

    val mergedPairs = adjPairs.mapPartitions(it =>
      it.toList.groupBy(x => x._1). map { case (k, v) =>
        (k, v.flatMap(_._2).sortBy(_.docId.rank)(Ordering[Double].reverse)) }.iterator,
      preservesPartitioning = true)

    mergedPairs
  }

  def loadGraphX(data: String): Graph[(VertexType), (EdgeType, Double, String)] = {
    val sc = Config.sc

    val rankRdd = loadRanks(sc, data)
    val adjRdd = loadAdjacents(sc, data)

    val vertexRdd: RDD[(VertexId, VertexType)] = rankRdd.map { case (id, _) => (id, PersonVertex) }

    val edgeRdd: RDD[Edge[(EdgeType, Double, String)]] = adjRdd.map { case (src, dest) =>
      Edge(src, dest, (FriendEdge, 0.0, "")) }

    Graph(vertexRdd, edgeRdd)
  }

  private def loadRanks(sc: SparkContext, data: String): RDD[(Int, Double)] = {
    val rankLines = sc.textFile(yfyHome + "/src/main/resources/" + data +"/" +
      data + rankSpec + fileExt)

    rankLines.map { rl =>
      val sp = rl.split(";")
      (sp(0).toInt, sp(1).toDouble)
    }
  }

  private def loadAdjacents(sc: SparkContext, data: String): RDD[(Int, Int)] = {
    val adjLines = sc.textFile(yfyHome + "/src/main/resources/" + data + "/" +
      data + adjSpec + fileExt)

    adjLines.map { adjl =>
      val sp = adjl.split(";")
      (sp(0).toInt, sp(1).toInt)
     }
  }
}

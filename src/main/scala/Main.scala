import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mini-unicorn").setMaster("local")
//    println("SparkConf: " + conf.toDebugString)
    val sc = new SparkContext(conf)

    val scGraph = GraphLoader.load(sc, "small-fake", 3)
    printPartitions(scGraph.postingLists)
    val first = scGraph.term(Term(FriendEdge, 5))
    println(first.collect.mkString(", "))
    val second = scGraph.term(Term(FriendEdge, 1))
    println(second.collect.mkString(", "))
    println(scGraph.or(List(first, second)).collect.mkString(", "))
    sc.stop
  }

  def printPartitions(rdd: RDD[_], first: Int = 100): Unit = {
    rdd.mapPartitionsWithIndex((index, it) =>
      it.toList.take(first).map(x => println(index + "->" + x)).iterator).collect
  }
}

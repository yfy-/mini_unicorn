import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mini_unicorn").setMaster("local")
    val sc = new SparkContext(conf)


//    val data = sc.parallelize(List((2, 2), (2, 3), (1, 4), (2, 5), (1, 6), (2, 7)))
//    val partitionedData = data.partitionBy(new RandomPartitioner(3))
//    printPartitions(partitionedData)
//    val merged = partitionedData.mapPartitions(it =>
//      it.toList.groupBy(x => x._1).map { case (k, v) => (k, v.map(_._2)) }.iterator, true)
//    printPartitions(merged)

    val lines = sc.textFile(getClass.getResource("small-fake.txt").getFile)
    val initialPairs = lines.map { l =>
      val x = l.split(" ")
      (Term(FriendEdge, x(0).toInt), Hit(DocId(PersonVertex, x(1).toInt, 0.0), "empty"))
    }.partitionBy(new RandomPartitioner(3))

    val mergedPairs = initialPairs.mapPartitions(it =>
      it.toList.groupBy(x => x._1).map { case (k, v) => (k, v.map(_._2)) }.iterator,
      true
    )

    val scGraph = new SocialGraph(mergedPairs)
    println(scGraph.term(Term(FriendEdge, 5)).collect().mkString)
//    val terms = List(Term(FriendEdge, 1), Term(FriendEdge, 2))
//    val firstHit = List(Hit(DocId(PersonVertex, 1, 1), "hitdata1"))
//    val secondHit = List(Hit(DocId(PersonVertex, 2, 5), "hitdata2"))
//    val listOfHits = List(firstHit, secondHit)
//
//    var x = DocId(PersonVertex, 2, 5)
//    val combined = terms.zip(listOfHits)
//    println(combined.mkString)
//    val graphRDD = sc.parallelize(combined)
//    val eliminated = graphRDD.filter { case (t, lh) =>
//      t.eType == FriendEdge && t.id == 2 }
//    val collected = eliminated.collect()
//    println(collected.mkString)
//    sc.stop
  }

  def printPartitions(rdd: RDD[_]): Unit = {
    rdd.mapPartitionsWithIndex((index, it) =>
      it.toList.map(x => println(index + "->" + x)).iterator).collect
  }
}

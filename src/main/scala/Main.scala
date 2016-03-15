import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mini_unicorn").setMaster("local")
    val sc = new SparkContext(conf)


    val data = sc.parallelize(List((2, 2), (2, 3), (1, 4), (2, 5), (1, 6), (2, 7)))
    val partitionedData = data.partitionBy(new RandomPartitioner(3))
    printPartitions(partitionedData)
    val merged = partitionedData.mapPartitions(it =>
      it.toList.groupBy(x => x._1).map { case (k, v) => (k, v.map(_._2)) }.toList.iterator, true)
    printPartitions(merged)
    sc.stop
  }

  def printPartitions(rdd: RDD[_]): Unit = {
    rdd.mapPartitionsWithIndex((index, it) =>
      it.toList.map(x => println(index + "->" + x)).iterator).collect
  }
}

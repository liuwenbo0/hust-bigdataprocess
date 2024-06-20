import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PageRankApp {
  def main(args: Array[String]): Unit = {
    // 设置 Spark 配置
    val conf = new SparkConf().setAppName("PageRank").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 加载图数据，边列表格式
    val lines = sc.textFile("hdfs://localhost:9000/user/hadoop/web-Stanford.txt")
    val links = lines.map { line =>
      val parts = line.split("\\s+")
      (parts(0).toLong, parts(1).toLong)
    }.distinct().groupByKey().cache()

    // 获取节点总数 N
    val N = links.count()

    // 初始化每个节点的 PageRank 值
    var ranks = links.mapValues(_ => 1.0 / N)

    // 定义阻尼系数和误差阈值
    val dampingFactor = 0.85
    val tolerance = 0.0001

    var delta = Double.MaxValue
    var iterations = 0

    // PageRank 迭代计算，直到收敛
    while (delta > tolerance) {
      val contributions = links.join(ranks).flatMap {
        case (id, (urls, rank)) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }

      val newRanks = contributions
        .reduceByKey(_ + _)
        .mapValues(v => (1 - dampingFactor) / N + dampingFactor * v)

      // 计算 PageRank 值的变化
      delta = ranks.join(newRanks)
        .map { case (id, (oldRank, newRank)) => math.abs(oldRank - newRank) }
        .sum()

      // 更新 ranks
      ranks = newRanks

      iterations += 1
      println(s"Iteration $iterations completed with delta = $delta")
    }

    // 显示结果
    ranks.collect().foreach { case (id, rank) => println(s"Node $id has rank $rank.") }

    // 停止 SparkContext
    sc.stop()
  }
}


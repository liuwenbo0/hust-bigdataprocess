import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class PageRank {
        public static void main(String[] args) {
                //set spark conf
                SparkConf conf = new SparkConf().setAppName("PageRank").setMaster("local[*]");
                JavaSparkContext sc = new JavaSparkContext(conf);

                //load graph data
                JavaRDD<String> lines = sc.textFile("hdfs://localhost:9000/user/hadoop/web-Stanford.txt")
                                                                  .zipWithIndex()
                                                                  .filter(tuple -> tuple._2 >= 4)
                                                                  .map(Tuple2::_1);

                //fromnode as key tonode as value
                JavaPairRDD<Long, Long> links = lines.mapToPair(line -> {
                        String[] parts = line.split("\\s+");
                        return new Tuple2<>(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
                });

                JavaPairRDD<Long, Iterable<Long>> groupedLinks = links.groupByKey().cache();
        // 获取节点总数 N
        long N = groupedLinks.keys().count();

        // 初始化每个节点的 PageRank 值
        JavaPairRDD<Long, Double> initialRanks = groupedLinks.mapValues(v -> 1.0 / N);

        // 定义阻尼系数和误差阈值
        double dampingFactor = 0.85;
        double tolerance = 0.0001;

        double delta = Double.MAX_VALUE;
        int maxIterations = 500;
        JavaPairRDD<Long, Double> ranks = initialRanks;
        int iterations = 0;

        // PageRank 迭代计算，直到收敛
        while (delta > tolerance && iterations < maxIterations) {
            JavaPairRDD<Long, Double> contributions = groupedLinks.join(ranks).flatMapToPair(tuple -> {
                long nodeId = tuple._1;
                Iterable<Long> neighbors = tuple._2._1;
                double rank = tuple._2._2;

                List<Tuple2<Long, Double>> results = new ArrayList<>(); // store the result
                int size = 0;
                for (Long neighbor : neighbors)
                {
                        size++;
                }
                for(Long neighbor : neighbors)
                {
                        results.add(new Tuple2<>(neighbor, rank / size));
                }
                return results.iterator();
            });

            JavaPairRDD<Long, Double> newRanks = contributions
                    .reduceByKey((Function2<Double, Double, Double>) (a, b) -> a + b)
                    .mapValues((org.apache.spark.api.java.function.Function<Double, Double>) v -> (1 - dampingFactor) / N + dampingFactor * v);

            // 计算 PageRank 值的变化
            delta = ranks.join(newRanks)
                  .mapToDouble(tuple -> Math.abs(tuple._2._1 - tuple._2._2))
                  .sum();


            // 更新PageRank值
            ranks = newRanks;

            iterations += 1;
            System.out.println("Iteration " + iterations + " completed with delta = " + delta);
        }

        // 显示结果
//        List<Tuple2<Long, Double>> output = ranks.collect();
//        for (Tuple2<Long, Double> tuple : output) {
//            System.out.println("Node " + tuple._1 + " has rank " + tuple._2 + ".");
//        }

        JavaRDD<String> result = ranks.map(tuple-> "Node " + tuple._1 + " had rank " + tuple._2);
        result.saveAsTextFile("/home/hadoop/eclipse-workspace/pageRank/pagerank-result");

        sc.stop();

        }
}

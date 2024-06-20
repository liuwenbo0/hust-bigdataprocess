# lab_record

大数据处理实验记录

## lab1-1

配置环境变量，方便运行指令

```bashrc
export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_162
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export SPARK_HOME=/usr/local/spark
export PATH=${JAVA_HOME}/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:/usr/local/sbt:$PATH
export TERM=xterm-color
```

启动 dfs

```cmd
start-dfs.sh
```

查看 hdfs 中的内容

```cmd
hdfs dfs -ls
```

从本机向 hdfs 中加入文件

```cmd
hdfs dfs -put $src $des
```

从 hdfs 中向本机传文件

```cmd
hdfs dfs -get $src $des
```

## lab1-2

按教程在 Eclipse 中编写 HDFSExample 项目，报错

```cmd
java.lang.RuntimeException: java.lang.ClassNotFoundException: Class org.apache.hadoop.hdfs.DistributedFileSystem not found
```

这是因为教程中给的 jar 包并不完整，需要在项目的 build path 中 configure buildpath 中再添加一个 hadoop-hdfs-client-xxx 包

编译 scala 代码的命令(注：项目需要满足 sbt 要求的文件结构，且在 simple.sbt 文件所在的同级目录运行 sbt 命令)

```cmd
sbt package
```

在执行`sbt package`时总是出现`java.lang.NumberFormatException: For input string: "0x100`报错，在.bashrc 中添加`export TERM=xterm-color`即可解决

任务书给的 scala 代码需要进行修改，具体就是用一个类把 import 后面的代码包起来，否则会报错
除此之外还应注意

1. 将 val buffer = new Array 修改为 val buffer = new Array[Byte](4096)
2. 将 val localFile 修改为 var,并且在每次循环中更新它
3. 报错 Class org.apache.hadoop.hdfs.DistributedFileSystem not found,在该项目的 simple.sbt 的依赖项中加上 hadop-common,hadoop-hdfs,hadoop-hdfs-client 包即可
4. 在运行时需要指定追加到开头还是末尾，sbt 运行指令形如`sbt "run end /user/hadoop/text.txt /mnt/hgfs/shared/a.txt"`
5. 与 8 类似，需要指定是否强制删除

在 spark 中运行 scala 代码的命令

```cmd
spark-submit [--class "文件名" 选填,如果有多个文件的话] $jar包位置
```

## lab1-3

在`/usr/local/spark`下执行`bin/spark-shell`命令打开 spark shell

如果直接像教程中那样 new 一个 SparkContext 对象会报错没有 SparkContext 对象
首先需要导入相关库

```cmd
import org.apache.spark.SparkContext
```

之后停止原有的 sc,`sc.stop()`,之后再执行`val sc = new SparkContext`即可

因为本地路径下没有文件`file:///home/stu/software/hadoop/README.txt`，所以使用`val textFile = sc.textFile("file:///mnt/hgfs/shared/a.txt")`指定本机下的 a.txt 文件

统计文件中各个单词的计数，得到结果：

```cmd
res3: Array[(String, Int)] = Array((T,1), (d,61), (z,2), (",36), (4,17), (8,6), (p,102), (L,6), (x,23), (R,10), (B,1), (6,6), (P,5), (t,152), (.,142), (0,7), (b,5), (h,68), (2,24), (" ",198), ($,16), ("",6), (>,16), (n,142), (f,25), (~,1), (j,10), (J,1), ((,32), (v,25), (F,4), (V,8), (:,42), (,,6), (l,116), (<,16), (N,10), (r,141), (D,3), (w,2), (),32), (=,9), (e,208), (s,131), (/,30), (M,18), (7,4), (5,4), (a,224), (_,3), (O,12), (;,1), (y,10), (A,13), (u,26), (#,2), (i,92), (I,13), (o,191), (k,33), (9,5), (3,15), (%,13), (K,1), (q,5), (-,16), (S,31), (C,28), (E,2), (1,24), (g,45), (W,3), (+,4), (c,99), (m,21))
```

执行命令`wordCounts.foreach(println)`得到结果

```cmd
scala> wordCounts.foreach(println)
(w,2)
(),32)
(=,9)
(e,208)
(s,131)
(/,30)
(M,18)
(7,4)
(5,4)
(a,224)
(_,3)
(O,12)
(;,1)
(y,10)
(A,13)
(u,26)
(#,2)
(i,92)
(I,13)
(o,191)
(k,33)
(9,5)
(3,15)
(%,13)
(T,1)
(d,61)
(z,2)
(",36)
(4,17)
(8,6)
(p,102)
(L,6)
(x,23)
(R,10)
(B,1)
(6,6)
(P,5)
(t,152)
(.,142)
(0,7)
(b,5)
(h,68)
(2,24)
( ,198)
($,16)
(,6)
(>,16)
(K,1)
(q,5)
(-,16)
(S,31)
(C,28)
(E,2)
(1,24)
(g,45)
(W,3)
(+,4)
(c,99)
(m,21)
(n,142)
(f,25)
(~,1)
(j,10)
(J,1)
((,32)
(v,25)
(F,4)
(V,8)
(:,42)
(,,6)
(l,116)
(<,16)
(N,10)
(r,141)
(D,3)
```

根据任务书中的参考代码 2 构建 scala 原生项目，直接运行会报错

```cmd
[error] /home/hadoop/sparkapp/wordCount/src/main/scala/wordCount.scala:1: object spark is not a member of package org.apache
[error] import org.apache.spark.rdd.RDD
[error]                   ^
[error] /home/hadoop/sparkapp/wordCount/src/main/scala/wordCount.scala:2: object spark is not a member of package org.apache
[error] import org.apache.spark.{SparkConf, SparkContext}
[error]                   ^
[error] two errors found
[error] (compile:compileIncremental) Compilation failed
[error] Total time: 4 s, completed Jun 13, 2024 9:22:49 AM
```

修改 sbt 文件加入`/usr/local/spark/jars`中的本地包依赖即可解决
修改后的`bulid.sbt`内容如下

```sbt
name := "Simple Project"

version := "1.0"

scalaVersion := "2.12.18"

val hadoopVersion = "3.1.3"
val hadoopOrg = "org.apache.hadoop"

// 定义一个函数来获取多个目录中的所有 JAR 文件
def jarDependencies(directories: Seq[File]): Seq[File] = {
  directories.flatMap { directory =>
    if (directory.exists && directory.isDirectory) {
      directory.listFiles.filter(_.getName.endsWith(".jar"))
    } else {
      Seq.empty[File]
    }
  }
}

/*
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  hadoopOrg % "hadoop-common" % hadoopVersion,
  hadoopOrg % "hadoop-nfs" % hadoopVersion,
  hadoopOrg % "hadoop-hdfs" % hadoopVersion,
  hadoopOrg % "hadoop-hdfs-nfs" % hadoopVersion,
  hadoopOrg % "hadoop-hdfs-client" % hadoopVersion
)
*/

// 指定三个目录
val hadoopLibDirectory1 = file("/usr/local/hadoop/share/hadoop/hdfs/lib")
val hadoopLibDirectory2 = file("/usr/local/hadoop/share/hadoop/common/lib")
val sparkLibDirectory = file("/usr/local/spark/jars")

unmanagedJars in Compile ++= {
  jarDependencies(Seq(hadoopLibDirectory1, hadoopLibDirectory2, sparkLibDirectory))
}

unmanagedJars in Compile ++= Seq(
  file("/usr/local/hadoop/share/hadoop/common/hadoop-common-3.1.3.jar"),
  file("/usr/local/hadoop/share/hadoop/common/hadoop-nfs-3.1.3.jar"),
  file("/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-3.1.3.jar"),
  file("/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-nfs-3.1.3.jar"),
  file("/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-client-3.1.3.jar")
)

resolvers ++= Seq(
  "Apache Releases" at "https://repository.apache.org/content/repositories/releases/",
  "Maven Central" at "https://repo1.maven.org/maven2/"
)
```

根据任务书中的参考代码 1 直接创建 scala 项目，运行也会报错，将路径修改为`val filePath = "/mnt/hgfs/shared/a.txt"`即可解决

## lab1-4

在 eclipse 中编写 java 代码

```java
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
```

代码思路：

- 首先指定在本地集群中运行。然后导入 web-Stanford.txt 的内容，将其中开头的 4 行丢掉(因为开头的四行是注释不是实际数据)。
- 之后将每一行用空格分割成两个部分（即 fromnode 和 tonode），用一个 pair 进行保存。然后将 fromnode 作为关键字进行合并，得到同一个 fromnode 的所有 tonode 的列表（同样是一个 pair，前者为 fromnode，后者是 tonode 构成的列表）。
- 对 fromnode 的不同值进行统计，即为图中所有有边节点的个数。
- 之后构造一个所有节点的初始的 pagerank 的数据结构，同样是一个 pair，前者为节点的编号，后者为 1/n，n 为上一步统计的节点的总数。
- 再定义阻尼系数为 0.85，误差阈值为 0.0001，最大迭代次数为 500，初始时的误差为 MAX。准备开始 pageRank 迭代过程。
- 迭代过程如下：
  - 因为每个出节点向其邻居均匀地贡献自身的 pagerank 值，故我们先将将节点的 rank 值与出节点建立映射，之后在第一个 for 循环中统计其邻居的数量，在第二个计算出节点对每个邻居的贡献值，用 flatMaptoPair 函数将所有节点对其每个邻居的贡献值合并到一个大的 resultRDD 中，即为 contributions。
  - 之后将 contributions 和原来的 rank 值用 mapreduce 方法进行运算得到更新后的 rank 值，再计算两次迭代中所有节点的 rank 值变化的绝对值之和，如果小于误差阈值就终止迭代过程。
- 最后将 rank 值的结果输出到项目目录下的 pagerank-result.txt 中。

为了方便地调用 spark 的接口，需要在项目的 build path 中 configure buildpath 中导入 hadoop 目录下的相关 jars 包和 spark/jars 目录下的所有 jars 包。值得注意的是，当出现多个 jars 包的不同版本时，运行时会报错类或方法的定义找不到，应该仔细检查导入的 jars 包看是否有重名的，将其中旧版本的 jars 包移除。（spark 和 hadoop 路径下的 jars 包有很多是相同的，但 hadoop 中的 jars 包一般版本会比较旧，一定要移除）

## 2

直接按照任务书中的代码运行会报错`Error initializing SparkContext :A master URL must be set in your configuration`
在任务指导书的参考代码的基础上指定在本地集群上运行

```scala
    val spark = SparkSession.builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()
```

教程中的 spark 和 scala 版本太老，要想运行应该修改 simple.sbt 为

```sbt
name := "Simple Project"
version := "1.0"
scalaVersion := "2.12.18"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"
```

在运行应用前要在另一终端先执行`nc -lk 9999`命令,使用 netcat 工具监听 9999 端口，否则执行应用可能会报`connection refused`错误
之后在另一终端输入`hello world, hello Beijing, hello world`即可，得到如下输出

```cmd
+--------+-----+
|   value|count|
+--------+-----+
|  world,|    1|
|   hello|    3|
|Beijing,|    1|
|   world|    1|
+--------+-----+
```

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
libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.12" % "3.5.1",
        "org.apache.spark" % "spark-sql_2.12" % "3.5.1",
        "org.apache.commons" % "commons-lang3" % "3.14.0"
        )

// 指定三个目录
val hadoopLibDirectory1 = file("/usr/local/hadoop/share/hadoop/hdfs/lib")
val hadoopLibDirectory2 = file("/usr/local/hadoop/share/hadoop/common/lib")

unmanagedJars in Compile ++= {
        jarDependencies(Seq(hadoopLibDirectory1, hadoopLibDirectory2))
}

resolvers ++= Seq(
        "Apache Releases" at "https://repository.apache.org/content/repositories/releases/",
        "Maven Central" at "https://repo1.maven.org/maven2/"
)

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Lab1_2_11 {
  def main(args: Array[String]): Unit = {
    // 创建Hadoop配置和文件系统对象
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(hadoopConf)

    try {
      // 指定源文件和目的文件路径
      val remoteFilePath = "/user/hadoop/text.txt"
      val remoteToFilePath = "/user/hadoop/new.txt"
      val srcPath = new Path(remoteFilePath)
      val dstPath = new Path(remoteToFilePath)

      // 移动文件
      val result = fs.rename(srcPath, dstPath)
      if (result) {
        println(s"文件 $remoteFilePath 已移动到 $remoteToFilePath")
      } else {
        println(s"操作失败（源文件不存在或移动失败）")
      }
    } finally {
      // 关闭文件系统
      fs.close()
    }
  }
}


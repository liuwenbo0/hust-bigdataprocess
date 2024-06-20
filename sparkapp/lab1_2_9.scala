import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object lab1_2_9{
  def main(args: Array[String]): Unit = {
    // 创建Hadoop配置和文件系统对象
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(hadoopConf)

    // 指定要删除的HDFS文件路径
    val remoteFilePath = "/user/hadoop/text.txt"
    val remotePath = new Path(remoteFilePath)

    // 删除文件
    val result = fs.delete(remotePath, false)
    if (result) {
      println(s"文件删除: $remoteFilePath")
    } else {
      println("操作失败（文件不存在或删除失败）")
    }

    // 关闭文件系统
    fs.close()
  }
}

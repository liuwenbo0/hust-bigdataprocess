import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

object Main {

  // 判断目录是否为空
  def isDirEmpty(conf: Configuration, remoteDir: String): Boolean = {
    val fs = FileSystem.get(conf)
    val dirPath = new Path(remoteDir)
    val remoteIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(dirPath, true)
    val isEmpty = !remoteIterator.hasNext
    fs.close()
    isEmpty
  }

  // 删除目录
  def rmDir(conf: Configuration, remoteDir: String, recursive: Boolean): Boolean = {
    val fs = FileSystem.get(conf)
    val dirPath = new Path(remoteDir)
    val result = fs.delete(dirPath, recursive)
    fs.close()
    result
  }

  def main(args: Array[String]): Unit = {
    // 创建Hadoop配置和文件系统对象
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")

    val remoteDir = "/user/hadoop/input"  // HDFS目录
    val forceDelete = true  // 是否强制删除

    if (!isDirEmpty(hadoopConf, remoteDir) && !forceDelete) {
      println("目录不为空，不删除")
    } else {
      if (rmDir(hadoopConf, remoteDir, forceDelete)) {
        println(s"目录已删除: $remoteDir")
      } else {
        println("操作失败")
      }
    }
  }
}


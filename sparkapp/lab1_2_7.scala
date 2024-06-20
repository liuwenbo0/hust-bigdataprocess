import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

object lab1_2_7 {
  def main(args: Array[String]): Unit ={ 
    // 创建Hadoop配置和文件系统对象
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(hadoopConf)

    // 检查路径是否存在
    def pathExists(conf: Configuration, path: String): Boolean = {
      val fs = FileSystem.get(conf)
      val exists = fs.exists(new Path(path))
      fs.close()
      exists
    }

    // 判断目录是否为空
    def isDirEmpty(conf: Configuration, remoteDir: String): Boolean = {
      val fs = FileSystem.get(conf)
      val dirPath = new Path(remoteDir)
      val remoteIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(dirPath, true)
      val isEmpty = !remoteIterator.hasNext
      fs.close()
      isEmpty
    }

    // 创建目录
    def mkdir(conf: Configuration, remoteDir: String): Boolean = {
      val fs = FileSystem.get(conf)
      val dirPath = new Path(remoteDir)
      val result = fs.mkdirs(dirPath)
      fs.close()
      result
    }

    // 删除目录
    def rmDir(conf: Configuration, remoteDir: String, recursive: Boolean): Boolean = {
      val fs = FileSystem.get(conf)
      val dirPath = new Path(remoteDir)
      val result = fs.delete(dirPath, recursive)
      fs.close()
      result
    }

    // 指定HDFS目录路径和是否强制删除
    val remoteDir = "/user/hadoop/input"
    val forceDelete = true  // true 表示强制删除，false 表示非空时不删除

    // 判断目录是否存在，不存在则创建，存在则删除
    if (!pathExists(hadoopConf, remoteDir)) {
      mkdir(hadoopConf, remoteDir)
      println(s"创建目录: $remoteDir")
    } else {
      if (isDirEmpty(hadoopConf, remoteDir) || forceDelete) {
        rmDir(hadoopConf, remoteDir, forceDelete)
        println(s"删除目录: $remoteDir")
      } else {
        println(s"目录不为空，不删除: $remoteDir")
      }
    }
  }
}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}

object lab1_2_6 {
  def main(args: Array[String]): Unit = {
    // 创建Hadoop配置和文件系统对象
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(hadoopConf)

    // 定义文件和目录路径
    val remoteFilePath = "/user/hadoop/input/text.txt"
    val remoteDir = "/user/hadoop/input"

    // 检查路径是否存在
    def pathExists(conf: Configuration, path: String): Boolean = {
      val fs = FileSystem.get(conf)
      val exists = fs.exists(new Path(path))
      fs.close()
      exists
    }

    // 创建目录
    def mkdir(conf: Configuration, remoteDir: String): Boolean = {
      val fs = FileSystem.get(conf)
      val dirPath = new Path(remoteDir)
      val result = fs.mkdirs(dirPath)
      fs.close()
      result
    }

    // 创建文件
    def touchz(conf: Configuration, remoteFilePath: String): Unit = {
      val fs = FileSystem.get(conf)
      val remotePath = new Path(remoteFilePath)
      val outputStream: FSDataOutputStream = fs.create(remotePath)
      outputStream.close()
      fs.close()
    }

    // 删除文件
    def rm(conf: Configuration, remoteFilePath: String): Boolean = {
      val fs = FileSystem.get(conf)
      val remotePath = new Path(remoteFilePath)
      val result = fs.delete(remotePath, false)
      fs.close()
      result
    }

    // 创建或删除文件
    if (pathExists(hadoopConf, remoteFilePath)) {
      rm(hadoopConf, remoteFilePath)
      println(s"文件已删除: $remoteFilePath")
    } else {
      if (!pathExists(hadoopConf, remoteDir)) {
        mkdir(hadoopConf, remoteDir)
        println(s"目录已创建: $remoteDir")
      }
      touchz(hadoopConf, remoteFilePath)
      println(s"文件已创建: $remoteFilePath")
    }
  }
}

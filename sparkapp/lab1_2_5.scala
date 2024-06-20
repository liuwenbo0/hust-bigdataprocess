import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import java.text.SimpleDateFormat

object lab1_2_5 {
  def main(args: Array[String]): Unit = {
    // 创建Hadoop配置和文件系统对象
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(hadoopConf)

    // 指定HDFS目录路径
    val remoteDir = "/user/hadoop"
    val dirPath = new Path(remoteDir)

    // 递归获取目录下的所有文件
    val remoteIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(dirPath, true)

    // 输出每个文件的信息
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    while (remoteIterator.hasNext()) {
      val fileStatus = remoteIterator.next()
      val path = fileStatus.getPath.toString
      val permissions = fileStatus.getPermission.toString
      val size = fileStatus.getLen
      val modificationTime = fileStatus.getModificationTime
      val modificationDate = dateFormat.format(modificationTime)
      
      println(s"Path: $path")
      println(s"Permissions: $permissions")
      println(s"Size: $size bytes")
      println(s"Modification Time: $modificationDate")
      println()
    }

    // 关闭文件系统
    fs.close()
  }
}

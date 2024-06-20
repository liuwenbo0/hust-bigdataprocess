import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileStatus, Path}
import java.text.SimpleDateFormat

object lab1_2_4 {
  def main(args: Array[String]): Unit = {
    // 创建Hadoop配置和文件系统对象
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(hadoopConf)

    // 指定HDFS文件路径
    val remoteFilePath = "/user/hadoop/text.txt"
    val remotePath = new Path(remoteFilePath)

    // 获取文件状态信息
    val fileStatus: FileStatus = fs.getFileStatus(remotePath)

    // 显示文件详细信息
    val permissions = fileStatus.getPermission
    val size = fileStatus.getLen
    val modificationTime = fileStatus.getModificationTime
    val path = fileStatus.getPath

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val modificationDate = dateFormat.format(modificationTime)

    println(s"Path: $path")
    println(s"Permissions: $permissions")
    println(s"Size: $size bytes")
    println(s"Modification Time: $modificationDate")

    // 关闭文件系统
    fs.close()
  }
}

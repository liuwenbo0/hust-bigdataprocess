import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.File

object lab1_2_2 {
  def main(args: Array[String]): Unit = {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(hadoopConf)

    val remoteFilePath = "/user/hadoop/file.txt"
    var localFilePath = "/mnt/hgfs/shared/a.txt"

    // 检查本地文件是否存在并重命名
    var localFile = new File(localFilePath)
    if (localFile.exists()) {
      println(s"$localFilePath 已存在.")
      var i = 0
      while (localFile.exists()) {
        localFilePath = s"/mnt/hgfs/shared/text_${i}.txt"
        i += 1
        localFile = new File(localFilePath)
      }
      println(s"将重新命名为: $localFilePath")
    }

    // 下载文件到本地
    val remotePath = new Path(remoteFilePath)
    val localPath = new Path(localFilePath)
    fs.copyToLocalFile(remotePath, localPath)
    println(s"File $remoteFilePath copied to $localFilePath.")

    fs.close()
  }
}


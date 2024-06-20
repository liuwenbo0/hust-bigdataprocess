import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import java.io.{BufferedInputStream, FileInputStream, File, IOException}

object lab1_2_8 {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: lab1_2_8 <beginning|end> <HDFS file path> <local file path>")
      sys.exit(1)
    }

    val appendPosition = args(0)
    val remoteFilePath = args(1)
    val localFilePath = args(2)

    // 创建Hadoop配置和文件系统对象
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(hadoopConf)

    try {
      val remotePath = new Path(remoteFilePath)

      if (appendPosition == "end") {
        // 追加内容到文件末尾
        val inputStream = new BufferedInputStream(new FileInputStream(localFilePath))
        val outputStream: FSDataOutputStream = fs.append(remotePath)
        val buffer = new Array[Byte](4096) 
        var bytesRead = inputStream.read(buffer)
        while (bytesRead > 0) {
          outputStream.write(buffer, 0, bytesRead)
          bytesRead = inputStream.read(buffer)
        }
        inputStream.close()
        outputStream.close()
        println(s"内容已追加到文件末尾: $remoteFilePath")
      } else if (appendPosition == "beginning") {
        // 追加内容到文件开头
        val localTmpPath = "/path/to/local/tmp.txt"
        val content = new BufferedInputStream(new FileInputStream(localFilePath))

        if (!fs.exists(remotePath)) {
          println(s"文件不存在: $remoteFilePath")
        } else {
          // 移动文件到本地
          fs.copyToLocalFile(remotePath, new Path(localTmpPath))
          fs.delete(remotePath, false)
          
          // 创建新文件并写入新内容
          val outputStream: FSDataOutputStream = fs.create(remotePath)
          val buffer = new Array[Byte](4096) 
          var bytesRead = content.read(buffer)
          while (bytesRead > 0) {
            outputStream.write(buffer, 0, bytesRead)
            bytesRead = content.read(buffer)
          }
          
          // 追加原内容到新文件
          val inputStream = new BufferedInputStream(new FileInputStream(localTmpPath))
          bytesRead = inputStream.read(buffer)
          while (bytesRead > 0) {
            outputStream.write(buffer, 0, bytesRead)
            bytesRead = inputStream.read(buffer)
          }
          inputStream.close()
          outputStream.close()
          println(s"内容已追加到文件开头: $remoteFilePath")
        }

        // 删除本地临时文件
        new File(localTmpPath).delete()
      } else {
        println("无效的追加位置参数，必须为 'beginning' 或 'end'")
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      // 关闭文件系统
      fs.close()
    }
  }
}

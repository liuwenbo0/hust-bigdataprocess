import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import java.io.{BufferedInputStream, FileInputStream}

object lab1_2_1 {
  def main(args: Array[String]): Unit = {
    // 创建Hadoop配置和文件系统对象
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(hadoopConf)

    val localFilePath = "/mnt/hgfs/shared/a.txt"  // 本地路径
    val hdfsFilePath = "/user/hadoop/file.txt"     // HDFS路径
    val choice = "append"  // 可选值: "append" 或 "overwrite"

    // 检查文件是否存在
    if (fs.exists(new Path(hdfsFilePath))) {
      if (choice == "append") {
        // 追加文件内容
        val inputStream = new BufferedInputStream(new FileInputStream(localFilePath))
        val outputStream: FSDataOutputStream = fs.append(new Path(hdfsFilePath))
        val buffer = new Array[Byte](4096)  // 定义 buffer 数组大小为 1024 字节
        var bytesRead = inputStream.read(buffer)
        while (bytesRead > 0) {
          outputStream.write(buffer, 0, bytesRead)

        }
        inputStream.close()
        outputStream.close()
        println(s"Content from $localFilePath appended to $hdfsFilePath.")
      } else if (choice == "overwrite") {
        // 覆盖文件
        fs.copyFromLocalFile(new Path(localFilePath), new Path(hdfsFilePath))
        println(s"File $localFilePath copied to $hdfsFilePath.")
      }
    } else {
      // 上传文件
      fs.copyFromLocalFile(new Path(localFilePath), new Path(hdfsFilePath))
      println(s"File $localFilePath copied to $hdfsFilePath.")
    }

    // 关闭文件系统
    fs.close()
  }
}


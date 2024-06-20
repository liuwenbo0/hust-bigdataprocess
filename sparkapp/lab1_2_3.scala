import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedReader, InputStreamReader}
object lab1_2_3 {
  def main(args: Array[String]): Unit = {
    // 创建Hadoop配置和文件系统对象
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
    val fs = FileSystem.get(hadoopConf)

    // 指定HDFS文件路径
    val remoteFilePath = "/user/hadoop/text.txt"
    val remotePath = new Path(remoteFilePath)

    // 打开HDFS文件并读取内容
    val inputStream = fs.open(remotePath)
    val reader = new BufferedReader(new InputStreamReader(inputStream))

    // 输出文件内容到终端
    var line: String = reader.readLine()
    while (line != null) {
      println(line)
      line = reader.readLine()
    }

    // 关闭资源
    reader.close()
    inputStream.close()
    fs.close()
  }
}

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author talent2333
 * Date 2020/7/17 14:58
 * Description
 * 自定义接收模式
 */
object demo_receiver_custom {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("testSparkStreaming")
      .setMaster("local[*]")
    //创建SparkStreaming程序执行入口
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //创建自定义接收器去接收数据流
    val lineStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 6666))

    //    lineStream
    //      .flatMap(_.split(" "))
    //      .map((_, 1))
    //      .reduceByKey(_ + _)
    //      .print
    //4.将每一行数据做切分，形成一个个单词
    val wordStream: DStream[String] = lineStream.flatMap(_.split("\t"))
    //5.将单词映射成元组（word,1）
    val wordAndOneStream: DStream[(String, Int)] = wordStream.map((_, 1))
    //6.将相同的单词次数做统计
    val wordAndCountStream: DStream[(String, Int)] = wordAndOneStream.reduceByKey(_ + _)
    //7.打印
    wordAndCountStream.print()

    //启动采集器
    ssc.start()
    //等待采集结束
    ssc.awaitTermination()


  }

}

//创建自定义接收类
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  private var socket: Socket = _

  override def onStart(): Unit = {
    // Start the thread that receives data over a connection

    try {
      socket = new Socket(host, port)
    } catch {
      case e: ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    }
    new Thread("Socket Receiver") {
      setDaemon(true)

      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def receive(): Unit = {

    //创建bufferedReader用于读取端口传来的数据
    val reader = new BufferedReader(new InputStreamReader(
      socket.getInputStream, StandardCharsets.UTF_8))
    //定义变量接收数据
    var input: String = null
    while ((input = reader.readLine()) != null) {
      store(input)
    }
  }

  override def onStop(): Unit = {
    synchronized {
      if (socket != null) {
        socket.close()
        socket = null
      }
    }
  }
}

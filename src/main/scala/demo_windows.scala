import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author talent2333
 * Date 2020/7/18 10:20
 * Description
 * windows operation
 */
object demo_windows {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("windows").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //设置检查点路径  用于保存状态
    ssc.checkpoint("cp")

    //创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop202", 6666)
    //设置窗口大小，滑动的步长
    val windowDS: DStream[String] = lineDStream.window(Seconds(6),Seconds(3))
    //扁平映射
    val flatMapDS: DStream[String] = windowDS.flatMap(_.split(" "))
    //结构转换
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))
    //聚合
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    reduceDS.print
    //启动
    ssc.start
    ssc.awaitTermination

  }

}

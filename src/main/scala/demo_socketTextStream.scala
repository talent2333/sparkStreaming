import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * Author talent2333
 * Date 2020/7/17 11:10
 * Description
 * 采集netcat的数据
 */
object demo_socketTextStream {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("testSparkStreaming").setMaster("local[*]")
    //创建SparkStreaming程序执行入口
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    //从端口读取数据
    val lineDs: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",6666,StorageLevel.MEMORY_ONLY)

    val flatDs: DStream[String] = lineDs.flatMap(_.split(" "))

    val mapDs: DStream[(String, Int)] = flatDs.map((_,1))

    val reduceDs: DStream[(String, Int)] = mapDs.reduceByKey(_+_)

    reduceDs.print()

    //启动采集器
    ssc.start()
    //等待采集结束
    ssc.awaitTermination()





  }

}

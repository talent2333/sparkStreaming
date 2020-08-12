import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * Author talent2333
 * Date 2020/7/18 9:37
 * Description ds->rdd
 */
object demo_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("testSparkStreaming").setMaster("local[*]")
    //创建SparkStreaming程序执行入口
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    //从端口读取数据
    val lineDs: ReceiverInputDStream[String] = ssc.
      socketTextStream("hadoop102",6666,StorageLevel.MEMORY_ONLY)

    val ds: DStream[(String, Int)] = lineDs.transform {
      rdd =>
        rdd
          .flatMap(_.split("\t"))
          .map((_, 1))
          .reduceByKey(_ + _)
          .sortByKey()
    }
    ds.print()
    //启动采集器
    ssc.start()
    //等待采集结束
    ssc.awaitTermination()
  }

}

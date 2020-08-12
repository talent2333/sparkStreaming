import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author talent2333
 * Date 2020/7/18 10:17
 * Description
 * UpdateStateByKey算子用于将历史结果应用到当前批次
 * 该操作允许在使用新信息不断更新状态的同时能够保留他的状态。
 */
object demo_updateStateByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    //设置检查点路径  用于保存状态
    ssc.checkpoint("cp")
    //创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop202", 6666)
    //扁平映射
    val flatMapDS: DStream[String] = lineDStream.flatMap(_.split(" "))
    //结构转换
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))
    //聚合
    // 注意：DStream中reduceByKey只能对当前采集周期（窗口）进行聚合操作，没有状态
    //val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)
    val stateDS: DStream[(String, Int)] = mapDS.updateStateByKey(
      (seq: Seq[Int], state: Option[Int]) => {
        Option(seq.sum + state.getOrElse(0))
      }
    )
    //打印输出
    stateDS.print()
    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * Author talent2333
 * Date 2020/7/17 14:22
 * Description
 * 通过RDD队列创建DStream
 */
object demo_rddQueue {
  def main(args: Array[String]): Unit = {

    //创建SPark配置文件
    val conf: SparkConf = new SparkConf()
      .setAppName("testSparkStreaming")
      .setMaster("local[*]")
    //创建SparkStreaming程序执行入口
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //create rdd queue
    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]
    //创建离散化流
    val queueStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)
    queueStream.map((_, 1)).reduceByKey(_ + _).print()

    //启动采集器
    ssc.start()

    //第0秒采集1条RDD，第3秒采集1条RDD，第6秒采集2条RDD，第9秒采集最后1条RDD
    for (i <- 1 to 5) {
      rddQueue.enqueue(ssc.sparkContext.makeRDD(1 to 5))
      Thread.sleep(2000)
    }


    //等待采集结束
    ssc.awaitTermination()
  }

}

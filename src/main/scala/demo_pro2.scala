import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * Author talent2333
 * Date 2020/7/18 15:51
 * Description
 * 最近一小时广告点击量实时统计
 */
object demo_pro2 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("topAds").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "xty",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    //collect data from kafka
    val lineDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
      .createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("ad_log"), kafkaParams))
    //设置检查点
    ssc.checkpoint("cp")
    //取得value的DS
    val valueDS: DStream[String] = lineDStream.map(_.value())
    //设置滑窗DS
    val winDS: DStream[String] = valueDS.window(Minutes(60),Seconds(6))
    //(date,product)
    val mapDS: DStream[(String, Int)] = winDS.map {
      data =>
        val strings: Array[String] = data.split(",")
        val date = new Date(strings(0).toLong)
        val sdf = new SimpleDateFormat("hh:mm")
        val date_formatted: String = sdf.format(date)
        val product: String = strings(4)
        (product+"_"+date_formatted,1)
    }
    //(product_date,num)
    val resDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)
    resDS.print

    //数据采集
    ssc.start()

    ssc.awaitTermination()

  }

}

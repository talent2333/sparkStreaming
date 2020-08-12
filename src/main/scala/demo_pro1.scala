import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author talent2333
 * Date 2020/7/18 14:47
 * Description
 * 每天每地区热门广告
 */
object demo_pro1 {
  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("topAds").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "xty",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    val lineDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
      .createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("ad_log"), kafkaParams))
    //设置检查点
    ssc.checkpoint("cp")
    //设置滑窗大小和滑窗步长
//    lineDStream.window()
    //获取kafka的value
    val lineValueDStream: DStream[String] = lineDStream.map(_.value())
    //切分数据得到对应数据(date_area_product,1)
    val dataDS: DStream[(String, Int)] = lineValueDStream.map {
      data =>
        val strings: Array[String] = data.split(",")
        val time: Long = strings(0).toLong
        val area: String = strings(1)
        val product: String = strings(4)
        val date = new java.util.Date(time)
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val date_formatted: String = simpleDateFormat.format(date)
        (date_formatted + "_" + area + "_" + product, 1)
    }
    //(date_area_product,sum)
    val sumDS: DStream[(String, Int)] = dataDS.updateStateByKey(
      (seq: Seq[Int], buffer: Option[Int]) => {
        Option(buffer.getOrElse(0) + seq.sum)
      }
    )
    //(date_area,product_sum)
    val cvtDS: DStream[(String, (String,Int))] = sumDS.map {
      case (data, sum) =>
        val datas: Array[String] = data.split("_")
        val k: String = datas(0) + "_" + datas(1)
        val v: (String, Int) =(datas(2),sum)
        (k, v)
    }
    //(date_area,lterable((product,sum))
    val grpDS: DStream[(String, Iterable[(String,Int)])] = cvtDS.groupByKey()
    //sort by sum by descending order then take top3 in each line
    val resDS: DStream[(String, List[(String, Int)])] = grpDS.mapValues(_.toList.sortBy(-_._2).take(3))

    resDS.print()

    ssc.start()
    ssc.awaitTermination()


  }

}

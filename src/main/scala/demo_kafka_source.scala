import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Author talent2333
 * Date 2020/7/17 16:15
 * Description
 * Kafka 0-10 Direct 模式
 */
object demo_kafka_source {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("testSparkStreaming").setMaster("local[*]")
    //创建SparkStreaming程序执行入口
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //3.构建Kafka参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "talent2333",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //读取kafka数据，创建DStream，设置消费者主题
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
      .createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("spark"), kafkaParams))
    //计算WordCount并打印
    kafkaDStream
      .map(_.value())
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()
    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }

}

package com.hd.bigdata

import java.text.SimpleDateFormat

import com.evergrande.hdmp.hbase.{ProjectConfig, RedisOperUtil}
import com.hd.bigdata.utils.{TransformerConfigure, DateUtils}
import org.apache.spark.{SparkConf, SparkContext}


object WordCount {

  // Usage: WordCount <local/yarn> <today:yyyy-MM-dd>
  def main(args: Array[String]) {

    // System.setProperty("hadoop.home.dir", "D:\\Program Files\\hadoop-common-2.2.0-bin-master");
    // sc.parallelize()
    // val line = sc.textFile("word.txt")
    // line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)

    if (args.length == 1 && args(0).equalsIgnoreCase("clear")) {
      println("Clearing existing Redis data ...")
      RedisOperUtil.clearRedisOldKey(ProjectConfig.KEY_PREFIX)
      println("Redis data cleared.")
      return
    }

    var conf: SparkConf = null

    if (args.length > 0 && args(0).equals("local")) {
      conf = new SparkConf().setAppName("CustomerMetrics").setMaster("local[4]")
    } else {
      conf = new SparkConf().setAppName("CustomerMetrics")
    }


    val sc = new SparkContext(conf)

    val today = new SimpleDateFormat("yyyy-MM-dd").parse(args(1))
    DateUtils.today = today

    val industryClassCode = args(2)
    val numPartitions = args(3).toInt

    TransformerConfigure.export2Redis = args(4).toBoolean
    TransformerConfigure.isDebug = args(5).toBoolean

    val transformer = new DataTransformer(sc, industryClassCode, today, numPartitions)

    val numIndex = transformer.produceNumIndex().cache()
    if (TransformerConfigure.isDebug) {
      println("Numeric Metrics Data Begin:")
      numIndex.take(100).foreach(println)
      println("Numeric Metrics Data End:")
    }

    val flagIndex = transformer.produceFlagIndex().cache()
    if (TransformerConfigure.isDebug) {
      println("Flag Metrics Data Begin:")
      flagIndex.take(100).foreach(println)
      println("Flag Metrics Data End:")
    }

    val commonIndex = transformer.produceCommonFlagIndex().cache()
    if (TransformerConfigure.isDebug) {
      println("Common Flag Metrics Data Begin:")
      commonIndex.take(100).foreach(println)
      println("Common Flag Metrics Data End:")
    }

    val finalIndex = transformer.produceFinalIndex(numIndex, flagIndex, commonIndex).cache()
    if (TransformerConfigure.isDebug) {
      println("Final Metrics Data Begin:")
      finalIndex.take(100).foreach(println)
      println("Final Metrics Data End:")
    }

    val discretIndex = transformer.produceDiscreteIndex(finalIndex).cache()
    if (TransformerConfigure.isDebug) {
      println("Discretized Metrics Data Begin:")
      discretIndex.take(100).foreach(println)
      println("Discretized Metrics Data End:")
    }

    if (args(0).equals("cluster")) {

      if (TransformerConfigure.export2Redis){
        println("Exporting discretized metrics to Redis ...")
        transformer.export2Redis(discretIndex)
        println("Discretized metrics exported to Redis.")
      }

      println("Exporting user metrics to HBase ...")
      transformer.export2HBase("user_metrics_test", finalIndex)
      println("Metrics data exported to HBase.")

      println("Exporting discretized metrics to HBase ...")
      transformer.export2HBase("user_discrete_metrics_test", discretIndex)
      println("Discretized metrics exported to HBase.")
    }

    sc.stop()
  }
}

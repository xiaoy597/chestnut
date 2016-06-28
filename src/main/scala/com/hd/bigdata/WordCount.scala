package com.hd.bigdata

import java.text.SimpleDateFormat

import com.hd.bigdata.utils.{TransformerConfigure, DateUtils}
import org.apache.spark.{SparkConf, SparkContext}


object WordCount {

  // Usage: WordCount <local/yarn> <today:yyyy-MM-dd>
  def main(args: Array[String]) {

    // System.setProperty("hadoop.home.dir", "D:\\Program Files\\hadoop-common-2.2.0-bin-master");
    // sc.parallelize()
    // val line = sc.textFile("word.txt")
    // line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)

    var conf: SparkConf = null
    val industryClassCode = args(2)

    if (args.length > 0 && args(0).equals("local")) {
      conf = new SparkConf().setAppName("CustomerMetrics").setMaster("local[4]")
    } else {
      conf = new SparkConf().setAppName("CustomerMetrics")
    }

    val sc = new SparkContext(conf)

    val today = new SimpleDateFormat("yyyy-MM-dd").parse(args(1))
    DateUtils.today = today

    val transformer = new DataTransformer(sc, industryClassCode, today)

    val numIndex = transformer.produceNumIndex()
    if (TransformerConfigure.isDebug) {
      println("Numeric Metrics Data Begin:")
      numIndex.take(100).foreach(println)
      println("Numeric Metrics Data End:")
    }

    val flagIndex = transformer.produceFlagIndex()
    if (TransformerConfigure.isDebug) {
      println("Flag Metrics Data Begin:")
      flagIndex.take(100).foreach(println)
      println("Flag Metrics Data End:")
    }

    val commonIndex = transformer.produceCommonFlagIndex()
    if (TransformerConfigure.isDebug) {
      println("Common Flag Metrics Data Begin:")
      commonIndex.take(100).foreach(println)
      println("Common Flag Metrics Data End:")
    }

    val finalIndex = transformer.produceFinalIndex(numIndex, flagIndex, commonIndex)
    if (TransformerConfigure.isDebug) {
      println("Final Metrics Data Begin:")
      finalIndex.take(100).foreach(println)
      println("Final Metrics Data End:")
    }

    if (args(0).equals("cluster")) {
      transformer.exportResult("user_metrics_test", finalIndex)
      println("User metrics data exported to HBase.")
    }

    val discretIndex = transformer.produceDiscreteIndex(finalIndex)
    if (TransformerConfigure.isDebug) {
      println("Discretized Metrics Data Begin:")
      discretIndex.take(100).foreach(println)
      println("Discretized Metrics Data End:")
    }

    if (args(0).equals("cluster")) {
      transformer.exportResult("user_discrete_metrics_test", discretIndex)
      println("Discretized user metrics data exported to HBase.")
    }

    sc.stop()
  }
}

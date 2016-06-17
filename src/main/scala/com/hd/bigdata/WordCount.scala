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
      conf = new SparkConf().setAppName("wordcount").setMaster("local[4]")
    } else {
      conf = new SparkConf().setAppName("wordcount")
    }

    val sc = new SparkContext(conf)

    val today = new SimpleDateFormat("yyyy-MM-dd").parse(args(1))
    DateUtils.today = today

    val transformer = new DataTransformer(sc, industryClassCode, today)

    val numIndex = transformer.produceNumIndex()
    if (TransformerConfigure.isDebug) {
      println("Numeric Index Data:")
      numIndex.take(100).foreach(println)
    }

    val flagIndex = transformer.produceFlagIndex()
    if (TransformerConfigure.isDebug) {
      println("Flag Index Data:")
      flagIndex.take(100).foreach(println)
    }

    val commonIndex = transformer.produceCommonFlagIndex()
    if (TransformerConfigure.isDebug) {
      println("Common Flag Index Data:")
      commonIndex.take(100).foreach(println)
    }

    val finalIndex = transformer.produceFinalIndex(numIndex, flagIndex, commonIndex)
    if (TransformerConfigure.isDebug) {
      println("Final Index Data:")
      finalIndex.take(100).foreach(println)
    }

    if (args(0).equals("cluster")) {
      transformer.exportResult("user_metrics_test", finalIndex)
      println("User metrics data exported to HBase.")
    }

    val discretIndex = transformer.produceDiscreteIndex(finalIndex)
    if (TransformerConfigure.isDebug) {
      println("Discrete Index Data:")
      discretIndex.take(100).foreach(println)
    }

    if (args(0).equals("cluster")) {
      transformer.exportResult("user_discrete_metrics_test", discretIndex)
      println("Discrete user metrics data exported to HBase.")
    }

    sc.stop()
  }
}

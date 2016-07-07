package com.hd.bigdata

import java.text.SimpleDateFormat

import com.evergrande.hdmp.hbase.{ProjectConfig, RedisOperUtil}
import com.hd.bigdata.utils.{TransformerConfigure, DateUtils}
import org.apache.spark.{SparkConf, SparkContext}


object MetricsLoader {

  def main(args: Array[String]) {

    if (args.length == 1 && args(0).equalsIgnoreCase("clear")) {
      println("Clearing existing Redis data ...")
      RedisOperUtil.clearRedisOldKey(ProjectConfig.KEY_PREFIX)
      println("Redis data cleared.")
      return
    }

    var conf: SparkConf = null

    if (args.length > 0 && args(0).equals("local")) {
      conf = new SparkConf().setAppName("MetricsLoad").setMaster("local[4]")
    } else {
      conf = new SparkConf().setAppName("指标与标签加载")
    }


    val sc = new SparkContext(conf)

    FlatConfig.indx_cat_cd = args(1)
    val today = new SimpleDateFormat("yyyy-MM-dd").parse(args(2))
    DateUtils.today = today

    FlatConfig.inds_cls_cd = args(3)
    val numPartitions = args(4).toInt

    TransformerConfigure.export2Redis = args(5).toBoolean
    TransformerConfigure.isDebug = args(6).toBoolean

    val transformer = new DataTransformer(sc, today, numPartitions)

    val metricsRDD = transformer.computeMetricsData()
    if (TransformerConfigure.isDebug){
      println("Metrics data for index category %s and industry %s has %d rows:"
        .format(FlatConfig.indx_cat_cd, FlatConfig.inds_cls_cd, metricsRDD.count()))
      metricsRDD.take(100).foreach(println)
    }

    val tagsRDD = transformer.computeTagData(metricsRDD)
    if (TransformerConfigure.isDebug){
      println("Tag data for index category %s and industry %s has %d rows:"
        .format(FlatConfig.indx_cat_cd, FlatConfig.inds_cls_cd, tagsRDD.count()))
      tagsRDD.take(100).foreach(println)
    }

    if (args(0).equals("cluster")) {

      if (TransformerConfigure.export2Redis){
        println("Exporting discretized metrics to Redis ...")
        transformer.export2Redis(tagsRDD)
        println("Discretized metrics exported to Redis.")
      }

      val indexCategory = FlatConfig.getIndexCategory()
        .filter(x => x.indx_cat_cd.equals(FlatConfig.indx_cat_cd)).head

      println("Exporting user metrics to HBase ...")
      transformer.export2HBase(indexCategory.metrics_tbl_nm, metricsRDD)
      println("Metrics data exported to HBase.")

      println("Exporting tags to HBase ...")
      transformer.export2HBase(indexCategory.tag_tbl_nm, tagsRDD)
      println("Tags exported to HBase.")
    }

    sc.stop()
  }
}

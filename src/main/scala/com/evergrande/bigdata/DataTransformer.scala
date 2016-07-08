package com.evergrande.bigdata

import java.util.Date

import com.evergrande.bigdata.utils.{HiveUtils, ProjectConfig, RedisOperUtil, TransformerConfigure}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
  * Created by Yu on 16/5/9.
  */

class DataTransformer(val sc: SparkContext, val today: Date, val numPartitions: Int) {

  def computeMetricsData(): RDD[(String, Map[String, String])] = {
    val rddList = ListBuffer[RDD[(String, Map[String, String])]]()

    for (config <- FlatConfig.getFlatRuleConfig()) {

      println("Getting index data from " + config._1.indx_tbl_nm + " for " + config._1.inds_cls_cd)

      val sql = FlatConfig.getIndexTableQueryStmt(config._1, config._2)

      println("SQL is [" + sql + "]")

      val dataFrame = HiveUtils.getDataFromHive(sql, sc)

      val rdd =
        if (config._1.flat_mode_cd.equals("20"))
          getMeasurableIndexData(dataFrame, config._1, config._2).cache()
        else
          getNonMeasurableIndexData(dataFrame, config._1, config._2).cache()

      if (TransformerConfigure.isDebug)
        if (rdd.count() > 0) {
          println("Index data from table " + config._1.indx_tbl_nm + " for industry " + config._1.inds_cls_cd + " is:")
          rdd.take(10).foreach(println)
        }

      rddList += rdd
    }

    rddList.reduce((rdd1, rdd2) =>
      rdd1.fullOuterJoin(rdd2).mapValues {
        case (Some(m1), Some(m2)) => m1 ++ m2
        case (None, Some(m)) => m
        case (Some(m), None) => m
        case _ => Map("Error" -> "Wrong result from full outer join!")
      }
    ).cache()
  }

  def getMeasurableIndexData(dataFrame: DataFrame, tableConfig: FlatTableConfig, columnConfig: List[FlatColumnConfig])
  : RDD[(String, Map[String, String])] = {

    val indexRule = columnConfig.groupBy(x => (x.statt_indx_id, x.dim_id)).mapValues(x => {
      for (c <- x) yield (c.indx_clmn_nm, c.indx_calc_mode_cd)
    })

    val rowWithRule = dataFrame.map(x => ((x.getAs[String]("statt_indx_id"), x.getAs[String](tableConfig.dim_clmn_nm)), x))
      .join(sc.parallelize(indexRule.toList))
      .map(x => (x._2._1.getAs[String](tableConfig.key_clmn_nm), x._2)).cache()

    if (TransformerConfigure.isDebug) {
      println("RowWithRule is:")
      rowWithRule.take(10).foreach(println)
    }

    val workdate = today

    val indexRDD = rowWithRule.combineByKey(
      i => MeasurableIndex(Map.empty, workdate, tableConfig).create(i),
      (r: MeasurableIndex, i) => r.augment(i),
      (r1: MeasurableIndex, r2: MeasurableIndex) => r1.merge(r2)
    ).mapValues(x => x.indexMap)

    indexRDD.mapValues(x => {
      for (m <- x) yield m._1 -> (if (m._2.toString().length == 0) "null" else m._2.toString)
    })
  }

  def getNonMeasurableIndexData(dataFrame: DataFrame, tableConfig: FlatTableConfig, columnConfig: List[FlatColumnConfig])
  : RDD[(String, Map[String, String])] = {

    val rdd = dataFrame.map(r => (r.getAs[String](tableConfig.key_clmn_nm), r)).mapValues(r => {
      (for (c <- columnConfig) yield
        tableConfig.indx_tbl_nm + "." + c.indx_clmn_nm -> {
          val v = r.get(r.fieldIndex(c.indx_clmn_nm))
          if (v == null || v.toString.length == 0) "null" else v.toString
        }).toMap
    }).cache()

    rdd
  }

  // Convert the user metrics to discrete value according to defined rules.
  def computeTagData(metricsRDD: RDD[(String, Map[String, String])])
  : RDD[(String, Map[String, String])] = {

    val dspsRules = FlatConfig.getDspsRule().map(
      x => x.flat_clmn_nm.toLowerCase ->(x.dsps_alg_type_cd, x.dsps_paras.split(','), x.tag_ctgy_id.toString)).toMap

    if (TransformerConfigure.isDebug) {
      println("Rules for generating tags: ")
      dspsRules.foreach(println)
    }

    val dspsMappingParas = FlatConfig.getDspsMappingPara()

    val mappingTables1 =
      dspsMappingParas.groupBy(_.dsps_mapping_para_class)
        .mapValues(list =>
          (for (p <- list) yield
            p.dsps_mapping_para_key -> p.dsps_mapping_para_value
            ).toMap
        )

    mappingTables1 match {
      case x: scala.collection.immutable.Map[String, Map[String, String]] =>
        println("Type Check OK")
      case _ => println("Wrong Type")
    }

    // The groupBy results in MapLike type that is not serializable, need to convert it back to normal Map.
    val mappingTables = Map() ++ mappingTables1

    mappingTables match {
      case x: scala.collection.immutable.Map[String, Map[String, String]] =>
        println("Type Check OK")
      case _ => println("Wrong Type")
    }

    if (TransformerConfigure.isDebug) {
      println("Mapping tables: " + mappingTables)
    }

    val tagRDD = metricsRDD.mapValues(indexMap => {
      val idxsToConvert = indexMap.keySet & dspsRules.keySet

      val tagList = for {
        idx <- idxsToConvert
        dsps_type_cd = dspsRules(idx)._1
        paramList = dspsRules(idx)._2
        tagName = "ctgy_" + dspsRules(idx)._3
        tagId = dspsRules(idx)._3
        idxValueStr = indexMap(idx)
      }
        yield {
          dsps_type_cd match {
            case "10" => // 按需求分段, 参数: 分段1下限, 分段1上限, 分段2下限, 分段2上限, ...
              val idxValueNum = BigDecimal(if (indexMap(idx).equals("null")) "0" else indexMap(idx))
              val hitSegList = for {
                i <- 0 until paramList.length / 2
                lowerBound = BigDecimal(paramList(2 * i))
                upperBound = BigDecimal(paramList(2 * i + 1))
                if idxValueNum > lowerBound && idxValueNum <= upperBound
              } yield Map(tagName -> (tagId + "%02d".format(i + 1)))
              if (hitSegList.isEmpty)
                Map(tagName -> (tagId + "%02d".format(paramList.length / 2 + 1)))
              else
                hitSegList.last

            case "40" => // 定长步进分段, 参数: 下限值, 上限值, 分段长度
              val lowerBound = BigDecimal(paramList(0))
              val upperBound = BigDecimal(paramList(1))
              val idxValueNum = BigDecimal(if (indexMap(idx).equals("null")) "0" else indexMap(idx))
              val stepValue = BigDecimal(paramList(2))
              if (idxValueNum < lowerBound || idxValueNum > upperBound)
                Map(tagName -> ("Out of bound -> " + indexMap(idx)))
              else
                Map(tagName -> (tagId + "%02d".format(((idxValueNum - lowerBound) / stepValue).toInt)))
            case "30" => // 键值映射/枚举, 参数: 映射表编号
              val mappingTableId = paramList(0)

              if (mappingTables.contains(mappingTableId)) {
                val mapping = mappingTables(mappingTableId)
                if (mapping.contains(idxValueStr))
                  Map(tagName -> (tagId + mapping(idxValueStr)))
                else
                  Map(tagName -> ("Missing value for key " + indexMap(idx)))
              } else
                Map(tagName -> ("Missing mapping class " + mappingTableId))

            case _ => // 未识别的离散模式
              Map(tagName -> ("Unknown dsps type of " + dsps_type_cd))
          }
        }

      if (tagList.nonEmpty)
        tagList.reduce((x1, x2) => x1 ++ x2)
      else
        Map[String, String]("XXX" -> "Missing")
    }).filter(_._2.nonEmpty).cache()

    if (TransformerConfigure.isDebug) {
      println("Records failed to be discretized: ")
      tagRDD
        .flatMap(r =>
          for (i <- r._2; if i._2.startsWith("Missing") || i._2.startsWith("Unknown")) yield i
        ).aggregateByKey(Set[String]())((u, v) => u + v, (u1, u2) => u1 ++ u2)
        .collect().foreach(println)
    }

    println("Statistics of tags:")
    tagRDD
      .flatMap(r => for (i <- r._2) yield (i._2, 1))
      .reduceByKey(_ + _)
      .collect().sortBy(_._1).foreach(println)

    tagRDD
  }

  def export2Redis(rdd: RDD[(String, Map[String, String])]): Unit = {
    if (TransformerConfigure.isDebug) {
      println("Number of partitions in RDD to be exported to Redis is " + rdd.partitions.length)
    }

    val rddToExport = {
      if (rdd.partitions.length > 32) {
        if (TransformerConfigure.isDebug)
          println("Repartition RDD to be exported to 16 partitions.")

        rdd.coalesce(16)
      } else
        rdd
    }.cache()

    val numTag2Redis = sc.accumulator(0)
    val numRow2Redis = sc.accumulator(0)

    // Write to Redis
    rddToExport.foreachPartition(partition => {
      try {
        val jedis = RedisOperUtil.getJedis
        if (jedis == null) {
          println("Failed to get Jedis resource!")
        } else {
          var pipeline = jedis.pipelined()
          var i: Int = 0
          partition.foreach(row => {
            for (metric <- row._2; redisKey = ProjectConfig.KEY_PREFIX + metric._2) {
              pipeline.setbit(redisKey, row._1.toLong, true)
              i += 1
              numTag2Redis += 1
              if (i == 1000000) {
                pipeline.sync()
                pipeline = jedis.pipelined()
                i = 0
              }
            }
            numRow2Redis += 1
          })

          pipeline.sync()
          RedisOperUtil.returnResource(jedis)
        }
      } catch {
        case e: Exception =>
          println("Exception raised in exporting tags to Redis.")
      }
    })

    println("Totally " + numRow2Redis.value + " rows with "
      + numTag2Redis.value + " tags written to Redis.")
  }

  def export2HBase(tableName: String, rdd: RDD[(String, Map[String, String])]): Unit = {

    if (TransformerConfigure.isDebug) {
      println("Number of partitions in RDD to be exported to HBase is " + rdd.partitions.length)
    }

    val rddToExport = {
      if (rdd.partitions.length > 32) {
        if (TransformerConfigure.isDebug)
          println("Repartition RDD to be exported to 16 partitions.")

        rdd.coalesce(16)
      } else
        rdd
    }.cache()

    val numCell2HBase = sc.accumulator(0)
    val numRow2HBase = sc.accumulator(0)

    // Write to HBase
    rddToExport.foreachPartition(partition => {
      val myConf = HBaseConfiguration.create()
      val myTable = new HTable(myConf, TableName.valueOf(tableName))
      myTable.setAutoFlush(false, false)
      myTable.setWriteBufferSize(64 * 1024 * 1024)
      partition.foreach(row => {

        val entry = new Put(Bytes.toBytes(row._1.toLong))

        val metricsMap = row._2
        if (metricsMap.nonEmpty) {
          metricsMap.foreach(metric => {
            entry.add("cf".getBytes,
              Bytes.toBytes(if (metric._1.length == 0) "null" else metric._1),
              Bytes.toBytes(if (metric._2.length == 0) "null" else metric._2))
            numCell2HBase += 1
          })
          myTable.put(entry)
        } else {
          println("WARNING!!! No metric/tag found for key " + row._1.toString)
        }

        numRow2HBase += 1
      })
      myTable.flushCommits()
    })

    println("Totally " + numRow2HBase.value + " rows with "
      + numCell2HBase.value + " cells written to HBase.")

  }
}

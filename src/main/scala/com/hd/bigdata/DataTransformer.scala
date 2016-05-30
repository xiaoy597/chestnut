package com.hd.bigdata

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.hd.bigdata.utils.TransformerConfigure
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Yu on 16/5/9.
  */

class DataTransformer(val sc: SparkContext, val industryClassCode: String, val today: Date) {

  val transformRules = FlatConfig.getFlatRuleConfig(null, industryClassCode).toList

  def getTransformRules: List[FlatRuleConfig] = {
    transformRules
  }

  def flattenTransformRulesForNumIndex(): RDD[((String, String), Iterable[(String, String)])] = {

    val transformRules = getTransformRules

    sc.parallelize(transformRules.filter(_.indx_tbl_nm.equals("h52_inds_statt_indx_rslt_g")))
      .map(r => ((r.dim_id, r.statt_indx_id), (r.indx_clmn_nm, r.indx_calc_mode_cd)))
      .groupByKey()
  }

  def flattenTransformRulesForFlagIndex(): List[(String, String, String)] = {

    val transformRules = getTransformRules

    transformRules.filter(!_.indx_tbl_nm.equals("h52_inds_statt_indx_rslt_g"))
      .flatMap(r => List((r.indx_tbl_nm, r.indx_clmn_nm, r.indx_calc_mode_cd)))
  }


  def combineIndexDataWithTransformRule(customerProdGrpIndexData: RDD[CustomerProdGrpIndexData],
                                        flatternedTransformRules: RDD[((String, String), Iterable[(String, String)])])
  : RDD[(String, (CustomerProdGrpIndexData, Iterable[(String, String)]))] = {

    //    implicit val sortingByCustIdAndDate = new Ordering[(String, String)] {
    //      def compare(a: (String, String), b: (String, String)) = {
    //        val x = a._1.compare(b._1)
    //        if (x == 0)
    //          a._2.compare(b._2)
    //        else
    //          x
    //      }
    //    }

    customerProdGrpIndexData
      .map(x => ((x.Prod_Grp_ID, x.Statt_Indx_ID), x))
      .join(flatternedTransformRules)
      .map(x => (x._2._1.Gu_Indv_Id, x._2))

    //      .join(providedIndexRDD.map(x => x match {
    //        case s: SoccerIndexData => (s.Gu_Indv_Id, s)
    //        case h: HotelIndexData => (h.gu_indv_id, h)
    //      }))
  }


  type CombinedIndexType = (CustomerProdGrpIndexData, Iterable[(String, String)])

  def produceNumIndex(): RDD[(String, Map[String, String])] = {

    val workdate = today

    val flatternedTransFormRulesForNumIndex = flattenTransformRulesForNumIndex()
    if (TransformerConfigure.isDebug) {
      println("Number of numeric flattened rules = " + flatternedTransFormRulesForNumIndex.count());
      flatternedTransFormRulesForNumIndex.collect().foreach(println)
    }

    val numIndexData = DataObtain.getIntegratedNumIndexFromHive(industryClassCode, sc)
    if (TransformerConfigure.isDebug) {
      println("Numeric Index Data:")
      numIndexData.take(100).foreach(println)
    }

    val combinedIndex = combineIndexDataWithTransformRule(
      numIndexData, flatternedTransFormRulesForNumIndex)

    if (TransformerConfigure.isDebug) {
      println("Numeric Index Data With Transformer Rules:")
      combinedIndex.take(100).foreach(println)
    }

    // The class field 'today' can't be referenced in this transformation since doing this will cause the
    // DataTransformer object to be serialized and dispatched to each task.
    val rdd1 = combinedIndex.combineByKey(
      i => NumIndexResult(Map.empty, workdate).create(i),
      (r: NumIndexResult, i) => r.augment(i),
      (r1: NumIndexResult, r2: NumIndexResult) => r1.merge(r2)
    ).mapValues(x => x.numIndices)

    rdd1.mapValues(x => {
      for (m <- x) yield m._1 -> m._2.toString()
    })
  }

  def produceFlagIndex(): RDD[(String, Map[String, String])] = {

    val flattenedTransformRulesForFlagIndex = flattenTransformRulesForFlagIndex()
    if (TransformerConfigure.isDebug) {
      println("Number of flag flattened rules = " + flattenedTransformRulesForFlagIndex.size)
      for (r <- flattenedTransformRulesForFlagIndex)
        println(r)
    }

    industryClassCode match {

      case "3110" => // Soccer
        val soccerIndexData = DataObtain.getIntegratedSoccerIndexFromHive(sc).map(x => (x.Gu_Indv_Id, x))
        //        val soccerIndex = TestData.getSoccerIndexData(sc).map(x => (x.Gu_Indv_Id, x))

        if (TransformerConfigure.isDebug) {
          println("Number of flag index data = " + soccerIndexData.count())
          soccerIndexData.take(100).foreach(println)
        }

        val soccerIndex = soccerIndexData.mapValues(soccerData => {
          val indexList = for ((idxTableName, idxColName, idxCalcMode) <- flattenedTransformRulesForFlagIndex)
            yield idxTableName ++ idxColName ->
              (idxColName match {
                case "Fst_Buy_Sig_Tkt_Dt" => soccerData.Fst_Buy_Sig_Tkt_Dt //首次购买单场票时间
                case "Lat_Buy_Sig_Tkt_Dt" => soccerData.Lat_Buy_Sig_Tkt_Dt // String  //最近一次购买单场票时间
                case "Fst_Buy_Vip_Tkt_Dt" => soccerData.Fst_Buy_Vip_Tkt_Dt // String  //首次购买套票时间
                case "Lat_Buy_Vip_Tkt_Dt" => soccerData.Lat_Buy_Vip_Tkt_Dt // String  //最近一次购买套票时间
                case "Fst_Buy_Shirt_Dt" => soccerData.Fst_Buy_Shirt_Dt // String  //首次购买球衣时间
                case "Lat_Buy_Shirt_Dt" => soccerData.Lat_Buy_Shirt_Dt // String  //最近一次购买球衣时间
                case "Fst_Buy_Souvenirs_Dt" => soccerData.Fst_Buy_Souvenirs_Dt // String  //首次购买纪念品时间
                case "Lat_Buy_Souvenirs_Dt" => soccerData.Lat_Buy_Souvenirs_Dt // String  //最近一次购买纪念品时间
                case "sig_reg_user_ind" => soccerData.sig_reg_user_ind.toString // Int  //是否单场注册用户
                case "vip_reg_user_ind" => soccerData.vip_reg_user_ind.toString // Int  //是否套票注册用户
                case "buy_sig_tkt_ind" => soccerData.buy_sig_tkt_ind.toString // Int  //是否购买过单场票
                case "buy_vip_tkt_ind" => soccerData.buy_vip_tkt_ind.toString // Int  //是否购买过套票
                case "buy_shirt_ind" => soccerData.buy_shirt_ind.toString // Int  //是否购买过球衣
                case "buy_souvenirs_ind" => soccerData.buy_souvenirs_ind.toString // Int  //是否购买过纪念品
                case "Cur_Vip_Score" => soccerData.Cur_Vip_Score.toString // Int  //当前积分
                case "Used_Vip_Score" => soccerData.Used_Vip_Score.toString // Int  //已用积分
                case "Accm_Vip_Score" => soccerData.Accm_Vip_Score.toString // Int  //累计积分
                case "Com_Recb_Addr" => soccerData.Com_Recb_Addr // String  //常用收货地址
                case _ => ""
              })
          indexList.filter(x => x._2.toString.length > 0).toMap
        })

        soccerIndex.filter(x => x._2.nonEmpty)

      case "2000" =>
        val hotelIndexData = DataObtain.getIntegratedHotelIndexFromHive(sc).map(x => (x.gu_indv_id, x))

        if (TransformerConfigure.isDebug) {
          println("Number of flag index data = " + hotelIndexData.count())
          hotelIndexData.take(100).foreach(println)
        }

        val hotelIndex = hotelIndexData.mapValues(hotelData => {
          val indexList = for ((idxTableName, idxColName, idxCalcMode) <- flattenedTransformRulesForFlagIndex)
            yield idxTableName ++ idxColName ->
              (idxColName match {
                case "gu_indv_id" => hotelData.gu_indv_id // string,
                case "hotel_mem_ind" => hotelData.hotel_mem_ind.toString // smallint,
                case "lodger_ind" => hotelData.lodger_ind.toString // smallint,
                case "bevrg_cust_ind" => hotelData.bevrg_cust_ind.toString // smallint,
                case "rest_mem_ind" => hotelData.rest_mem_ind.toString // smallint,
                case "recrn_cust_ind" => hotelData.recrn_cust_ind.toString // smallint,
                case "sport_mem_ind" => hotelData.sport_mem_ind.toString // smallint,
                case "memb_crd_qty" => hotelData.memb_crd_qty.toString // int,
                case "point_bal" => hotelData.point_bal.toString // int,
                case "accm_point" => hotelData.accm_point.toString // int,
                case "mem_crd_bal" => hotelData.mem_crd_bal.toString // decimal(18,2),
                case "mem_crd_accm_rec_amt" => hotelData.mem_crd_accm_rec_amt.toString // decimal(18,2),
                case "mem_crd_accm_consm_amt" => hotelData.mem_crd_accm_consm_amt.toString // decimal(18,2),
                case "hotel_urbn_qty" => hotelData.hotel_urbn_qty.toString // int,
                case "room_bookn_ttl_cnt" => hotelData.room_bookn_ttl_cnt.toString // int,
                case "live_ttl_cnt" => hotelData.live_ttl_cnt.toString // int,
                case "bookn_live_ttl_days" => hotelData.bookn_live_ttl_days.toString // int,
                case "live_ttl_days" => hotelData.live_ttl_days.toString // int,
                case "avg_live_days" => hotelData.avg_live_days.toString // int,
                case "max_live_days" => hotelData.max_live_days.toString // int,
                case "max_live_in_cnt" => hotelData.max_live_in_cnt.toString // int,
                case "live_consm_amt" => hotelData.live_consm_amt.toString // decimal(18,2),
                case "rest_consm_amt" => hotelData.rest_consm_amt.toString // decimal(18,2),
                case "met_consm_amt" => hotelData.met_consm_amt.toString // decimal(18,2),
                case "spa_consm_amt" => hotelData.spa_consm_amt.toString // decimal(18,2),
                case "sport_consm_amt" => hotelData.sport_consm_amt.toString // decimal(18,2),
                case "ent_consm_amt" => hotelData.ent_consm_amt.toString // decimal(18,2),
                case "oth_consm_amt" => hotelData.oth_consm_amt.toString // decimal(18,2),
                case "consm_totl_amt" => hotelData.consm_totl_amt.toString // decimal(18,2),
                case "hyear_total_amt" => hotelData.hyear_total_amt.toString // decimal(18,2),
                case "mon_total_amt" => hotelData.mon_total_amt.toString // decimal(18,2),
                case "wek_total_amt" => hotelData.wek_total_amt.toString // decimal(18,2),
                case "avg_consm_amt" => hotelData.avg_consm_amt.toString // decimal(18,2),
                case "with_child_ind" => hotelData.with_child_ind.toString // int,
                case "ear_live_dt" => hotelData.ear_live_dt // string,
                case "rec_live_dt" => hotelData.rec_live_dt // string,
                case "rec_hyear_live_cnt" => hotelData.rec_hyear_live_cnt.toString // int,
                case "rec_mon_live_cnt" => hotelData.rec_mon_live_cnt.toString // int,
                case "rec_wek_live_cnt" => hotelData.rec_wek_live_cnt.toString // int,
                case "room_typ_cd" => hotelData.room_typ_cd // string,
                case "com_bookn_chnl" => hotelData.com_bookn_chnl // string,
                case "com_pay_typ" => hotelData.com_pay_typ // string,
                case "com_bookn_cust_typ_cd" => hotelData.com_bookn_cust_typ_cd // string,
                case "now_diff_ear" => hotelData.now_diff_ear.toString // int,
                case "now_diff_rec" => hotelData.now_diff_rec.toString // int,
                case "rec_diff_ear" => hotelData.rec_diff_ear.toString // int,
                case "ear_week_day" => hotelData.ear_week_day // string,
                case "ear_mon" => hotelData.ear_mon // string,
                case "data_dt" => hotelData.data_dt // string,
                case _ => ""
              })
          indexList.filter(x => x._2.toString.length > 0).toMap
        })

        hotelIndex.filter(x => x._2.nonEmpty)

      case _ => sc.emptyRDD
    }
  }

  def produceFinalIndex(numIndexRDD: RDD[(String, Map[String, String])],
                        flagIndexRDD: RDD[(String, Map[String, String])])
  : RDD[(String, Map[String, String])] = {

    numIndexRDD.cogroup(flagIndexRDD).mapValues(x => {
      var finalMap = Map[String, String]()
      for (index <- x._1)
        finalMap = finalMap ++ index
      for (index <- x._2)
        finalMap = finalMap ++ index

      finalMap
    })
  }

  // Convert the user metrics to discrete value according to defined rules.
  def produceDiscreteIndex(originalIndex: RDD[(String, Map[String, String])])
  : RDD[(String, Map[String, String])] = {

    val dspsRules = FlatConfig.getDspsRule().map(
      x => x.flat_clmn_nm ->(x.dsps_alg_type_cd, x.dsps_rules.split(','))).toMap

    if (TransformerConfigure.isDebug) {
      println("Rules for generating discrete metrics: ")
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

    originalIndex.mapValues(indexMap => {
      val idxsToConvert = indexMap.keySet & dspsRules.keySet

      if (idxsToConvert.isEmpty) indexMap
      else {
        val discretIdx = (
          for {
            idx <- idxsToConvert
            dsps_type_cd = dspsRules(idx)._1
            paramList = dspsRules(idx)._2
            idxValueNum = BigDecimal(indexMap(idx))
            idxValueStr = indexMap(idx)
          }
            yield {
              dsps_type_cd match {
                case "10" => // 按需求分段, 参数: 分段1下限, 分段1上限, 分段2下限, 分段2上限, ...
                  (for {
                    i <- 0 until paramList.length / 2
                    lowerBound = BigDecimal(paramList(2 * i))
                    upperBound = BigDecimal(paramList(2 * i + 1))
                    if idxValueNum > lowerBound && idxValueNum <= upperBound
                  } yield idx -> (i + 1).toString).toMap
                case "40" => // 定长步进分段, 参数: 下限值, 上限值, 分段长度
                  val lowerBound = BigDecimal(paramList(0))
                  val upperBound = BigDecimal(paramList(1))
                  val stepValue = BigDecimal(paramList(2))
                  if (idxValueNum < lowerBound || idxValueNum > upperBound)
                    Map(idx -> indexMap(idx))
                  else
                    Map(idx -> ((idxValueNum - lowerBound) / stepValue).toInt.toString)
                case "30" => // 键值映射/枚举, 参数: 映射表编号
                  val mappingTableId = paramList(0)

                  if (mappingTables.contains(mappingTableId)) {
                    val mapping = mappingTables(mappingTableId)
                    if (mapping.contains(idxValueStr))
                      Map(idx -> mapping(idxValueStr))
                    else
                      Map(idx -> indexMap(idx))
                  } else
                    Map(idx -> indexMap(idx))

                case _ => // 未识别的离散模式
                  Map(idx -> indexMap(idx))
              }
            }).reduce((x1, x2) => x1 ++ x2)

        indexMap ++ discretIdx
      }
    })
  }

  def exportResult(tableName: String, finalResult: RDD[(String, Map[String, String])]): Unit = {

    if (TransformerConfigure.isDebug) {
      val badRecordSet = finalResult.filter(x => x._1 == null || x._2 == null || x._2.isEmpty)
      val badCount = badRecordSet.count()
      if (badCount > 0) {
        println("There are " + badCount + " metrics records have empty field set, write to HBase is canceled.")
        val badCount1 = badRecordSet.filter(x => x._1 == null).count()
        val badCount2 = badRecordSet.filter(x => x._2 == null).count()
        val badCount3 = badRecordSet.filter(x => x._2.isEmpty).count()
        println("BadCount1=" + badCount1 + ", BadCount2=" + badCount2 + ", BadCount3=" + badCount3)
        if (badCount1 > 0) {
          println("Bad1")
          badRecordSet.filter(x => x._1 == null).collect().foreach(println)
        }
        if (badCount2 > 0) {
          println("Bad2")
          badRecordSet.filter(x => x._2 == null).collect().foreach(println)
        }
        if (badCount3 > 0) {
          println("Bad3")
          badRecordSet.filter(x => x._2.isEmpty).collect().foreach(println)
        }
        return
      }
    }

    finalResult.foreachPartition(x => {
      val myConf = HBaseConfiguration.create()
      val myTable = new HTable(myConf, TableName.valueOf(tableName))
      myTable.setAutoFlush(false, false)
      myTable.setWriteBufferSize(10 * 1024 * 1024)
      x.foreach(a => {

        val p = new Put(Bytes.toBytes(a._1))
        val indices = a._2
        if (indices.nonEmpty) {
          indices.foreach(b => {
            p.add("cf".getBytes, Bytes.toBytes(b._1), Bytes.toBytes(b._2))
          })
          myTable.put(p)
        } else {
          println("WARNING!!! No metrics found for " + a._1.toString)
        }
      })
      myTable.flushCommits()
    })
  }
}

case class NumIndexResult(numIndices: Map[String, BigDecimal], today: Date) {
  type CombinedIndexType = (CustomerProdGrpIndexData, Iterable[(String, String)])
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  //  var today = dateFormat.parse("2016-05-10")

  def getDateBeforeDays(nDays: Int): Date = {
    val rightNow = Calendar.getInstance()

    rightNow.setTime(today)

    rightNow.add(Calendar.DAY_OF_YEAR, -nDays)

    rightNow.getTime
  }

  val date7DaysBefore = getDateBeforeDays(7)
  val date30DaysBefore = getDateBeforeDays(30)

  def create(combinedIndex: CombinedIndexType): NumIndexResult = {

    var numIndices = Map[String, BigDecimal]()

    for (indexRule <- combinedIndex._2) {
      numIndices = updateIndexValue(combinedIndex._1, indexRule, numIndices)
    }

    NumIndexResult(numIndices, today)
  }

  def augment(combinedIndex: CombinedIndexType): NumIndexResult = {
    var numIndices = this.numIndices

    for (indexRule <- combinedIndex._2) {
      numIndices = updateIndexValue(combinedIndex._1, indexRule, numIndices)
    }

    NumIndexResult(numIndices, today)
  }

  def merge(otherResult: NumIndexResult): NumIndexResult = {
    NumIndexResult(numIndices ++ otherResult.numIndices, today)
  }

  def updateIndexValue(customerProdGrpIndexData: CustomerProdGrpIndexData,
                       indexRule: (String, String),
                       indexValues: Map[String, BigDecimal]
                      ): Map[String, BigDecimal] = {
    println("The date str to parse = [" + customerProdGrpIndexData.Statt_Dt + "]")
    val indexDate = dateFormat.parse(customerProdGrpIndexData.Statt_Dt)
    val indexCalcMode = indexRule._2

    val targetIndexName = "idx_" ++
      customerProdGrpIndexData.Prod_Grp_ID ++
      customerProdGrpIndexData.Statt_Indx_ID ++
      indexRule._1 ++
      indexCalcMode

    val indexValue: BigDecimal = indexRule._1 match {
      case "Day_Indx_Val" => customerProdGrpIndexData.Day_Indx_Val
      case "Month_Indx_Val" => customerProdGrpIndexData.Month_Indx_Val
      case "Quarter_Indx_val" => customerProdGrpIndexData.Quarter_Indx_val // Note: the 'v' is lower case.
      case "Year_Indx_Val" => customerProdGrpIndexData.Year_Indx_Val
      case "Accm_Indx_Val" => customerProdGrpIndexData.Accm_Indx_Val
      case "Rec_60d_Indx_Val" => customerProdGrpIndexData.Rec_60d_Indx_Val
      case "Rec_90d_Indx_Val" => customerProdGrpIndexData.Rec_90d_Indx_Val
      case "Rec_180d_Indx_Val" => customerProdGrpIndexData.Rec_180d_Indx_Val
      case "Rec_360d_Indx_Val" => customerProdGrpIndexData.Rec_360d_Indx_Val
      case _ => 0
    }

    indexCalcMode match {
      case "10" if indexDate.equals(today) => // Directly assignment for indices of current date.
        indexValues + (targetIndexName -> indexValue)
      case "20" if indexDate.after(date7DaysBefore) => // Aggregate on last 7 days.
        if (indexValues.contains(targetIndexName))
          indexValues + (targetIndexName -> (indexValue + indexValues(targetIndexName)))
        else
          indexValues + (targetIndexName -> indexValue)
      case "21" if indexDate.after(date30DaysBefore) => // Aggregate on last 30 days.
        if (indexValues.contains(targetIndexName))
          indexValues + (targetIndexName -> (indexValue + indexValues(targetIndexName)))
        else
          indexValues + (targetIndexName -> indexValue)
      case _ => indexValues
    }

  }
}
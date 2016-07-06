package com.hd.bigdata

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.evergrande.hdmp.hbase.{ProjectConfig, RedisOperUtil}
import com.hd.bigdata.utils.{HiveUtils, TransformerConfigure}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}

import scala.collection.mutable.ListBuffer

/**
  * Created by Yu on 16/5/9.
  */

class DataTransformer(val sc: SparkContext, val today: Date, val numPartitions: Int) {

  //  val transformRules = FlatConfig.getFlatRuleConfig()

  def getIndexData() = {
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
          sc.emptyRDD[(String, Map[String, String])]


      if (TransformerConfigure.isDebug)
        if (rdd.count() > 0) {
          println("Index data from table " + config._1.indx_tbl_nm + " for industry " + config._1.inds_cls_cd + " is:")
          rdd.take(10).foreach(println)
        }

      rddList += rdd
    }
  }

  def getMeasurableIndexData(dataFrame: DataFrame, tableConfig: FlatTableConfig, columnConfig: List[FlatColumnConfig])
  : RDD[(String, Map[String, String])] = {

    val indexRule = columnConfig.groupBy(x => (x.statt_indx_id, x.dim_id)).mapValues(x => {
      for (c <- x) yield (c.indx_clmn_nm, c.indx_calc_mode_cd)
    })

    val rowWithRule = dataFrame.map(x => ((x.getAs[String]("statt_indx_id"), x.getAs[String](tableConfig.dim_clmn_nm)), x))
      .join(sc.parallelize(indexRule.toList))
      .map(x => (x._2._1.getAs[String](tableConfig.key_clmn_nm), x._2)).cache()

    if (TransformerConfigure.isDebug){
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
      for (m <- x) yield m._1 -> m._2.toString()
    })
  }


  def getTransformRules: List[FlatRuleConfig] = {
    //    transformRules
    List()
  }

  def flattenTransformRulesForNumIndex(): List[((String, String), Iterable[(String, String)])] = {

    val transformRules = getTransformRules

    //    sc.parallelize(transformRules.filter(_.indx_tbl_nm.equals("h52_inds_statt_indx_rslt_g")))
    //      .map(r => ((r.dim_id, r.statt_indx_id), (r.indx_clmn_nm, r.indx_calc_mode_cd)))
    //      .groupByKey()

    transformRules.filter(x => x.indx_tbl_nm.equals("h52_inds_statt_indx_rslt_g"))
      .groupBy(r => (r.dim_id, r.statt_indx_id))
      .mapValues(l => {
        for (r <- l) yield (r.indx_clmn_nm, r.indx_calc_mode_cd)
      }).toList
  }

  def flattenTransformRulesForFlagIndex(): List[(String, String, String, String)] = {

    val transformRules = getTransformRules

    transformRules.filter(x => !x.indx_tbl_nm.equals("h52_inds_statt_indx_rslt_g"))
      .flatMap(r => List((r.indx_tbl_nm, r.indx_clmn_nm, r.inds_cls_cd, r.indx_calc_mode_cd)))
  }


  def combineIndexDataWithTransformRule(customerProdGrpIndexData: RDD[CustomerProdGrpIndexData],
                                        flatternedTransformRules: RDD[((String, String), Iterable[(String, String)])])
  : RDD[(Long, (CustomerProdGrpIndexData, Iterable[(String, String)]))] = {

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

  def produceNumIndex(): RDD[(Long, Map[String, String])] = {

    val workdate = today

    val flatternedTransFormRulesForNumIndex = flattenTransformRulesForNumIndex()
    if (TransformerConfigure.isDebug) {
      println("Number of numeric flattened rules = " + flatternedTransFormRulesForNumIndex.length)
      flatternedTransFormRulesForNumIndex.foreach(println)
    }

    val numIndexData0 = DataObtain.getIntegratedNumIndexFromHive(FlatConfig.inds_cls_cd, sc)
    if (TransformerConfigure.isDebug) {
      println("Number of partitions for numeric index data is: " + numIndexData0.partitions.length)
    }

    val numIndexData = {
      if (numIndexData0.partitions.length > numPartitions * 4 || numIndexData0.partitions.length < numPartitions / 4) {
        if (TransformerConfigure.isDebug) {
          println("Repartition numeric index data from " +
            numIndexData0.partitions.length + " partitions to " + numPartitions + " partitions.")
        }
        numIndexData0.repartition(numPartitions).cache()
      } else
        numIndexData0.cache()
    }

    if (TransformerConfigure.isDebug) {
      println("Numeric Souce Data Begin:")
      numIndexData.take(100).foreach(println)
      println("Numeric Source Data End:")
    }

    val combinedIndex = combineIndexDataWithTransformRule(
      numIndexData, sc.parallelize(flatternedTransFormRulesForNumIndex)).cache()

    if (TransformerConfigure.isDebug) {
      println("Numeric Source Data With Transformer Rules Begin:")
      combinedIndex.take(100).foreach(println)
      println("Numeric Source Data With Transformer Rules End:")
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

  def produceCommonFlagIndex(): RDD[(Long, Map[String, String])] = {

    val flattenedTransformRulesForFlagIndex = flattenTransformRulesForFlagIndex().filter(x => x._1.equals("h52_cust_inds_merge"))
    if (TransformerConfigure.isDebug) {
      println("Number of common flag flattened rules = " + flattenedTransformRulesForFlagIndex.size)
      for (r <- flattenedTransformRulesForFlagIndex)
        println(r)
    }

    if (flattenedTransformRulesForFlagIndex.isEmpty) {
      return sc.emptyRDD
    }

    val commonIndexData0 = DataObtain.getIntegratedCommonIndexFromHive(sc, FlatConfig.inds_cls_cd).map(x => (x.gu_indv_id, x))
    if (TransformerConfigure.isDebug) {
      println("Number of partitions for common flag index data is: " + commonIndexData0.partitions.length)
    }

    val commonIndexData = {
      if (commonIndexData0.partitions.length > numPartitions * 2 || commonIndexData0.partitions.length < numPartitions / 2) {
        if (TransformerConfigure.isDebug) {
          println("Repartition common flag index data from " +
            commonIndexData0.partitions.length + " partitions to " + numPartitions + " partitions.")
        }
        commonIndexData0.repartition(numPartitions).cache()
      } else {
        commonIndexData0.cache()
      }
    }

    if (TransformerConfigure.isDebug) {
      println("Number of common index data = " + commonIndexData.count())
      commonIndexData.take(100).foreach(println)
    }

    val commonIndex = commonIndexData.mapValues(commonData => {
      val indexList = for ((idxTableName, idxColName, idxIndusClsCode, idxCalcMode) <- flattenedTransformRulesForFlagIndex)
        yield idxTableName + "." + idxColName ->
          (idxColName match {
            case "gu_indv_id" => commonData.gu_indv_id.toString //  String, // 个人客户统一编号
            case "cust_nm" => commonData.cust_nm //  String, // 客户姓名
            case "idtfy_info" => commonData.idtfy_info //  String, // 身份证号码
            case "birth_dt" => commonData.birth_dt //  String, // 出生日期
            case "cust_age" => commonData.cust_age.toString //  Int, // 客户年龄
            case "gender_cd" => commonData.gender_cd //  String, // 性别代码
            case "admin_regn_id" => commonData.admin_regn_id //  String, // 行政区域编号
            case "prov_id" => commonData.prov_id //  String, // 归属省份编号
            case "cty_id" => commonData.cty_id //  String, // 归属城市编号
            case "mobl_num1" => commonData.mobl_num1 //  String, // 手机号码1
            case "mobl_num2" => commonData.mobl_num2 //  String, // 手机号码2
            case "mobl_num3" => commonData.mobl_num3 //  String, // 手机号码3
            case "family_tel" => commonData.family_tel //  String, // 家庭电话
            case "offi_tel" => commonData.offi_tel //  String, // 办公电话
            case "email" => commonData.email //  String, // 行业代码
            case "marrg_stat_cd" => commonData.marrg_stat_cd //  String, // 邮箱
            case "edu_degree_cd" => commonData.edu_degree_cd //  String, // 婚姻状态代码
            case "industry_cd" => commonData.industry_cd //  String, // 学历代码
            case "job_cd" => commonData.job_cd //  String, // 职业代码
            case "family_addr" => commonData.family_addr //  String, // 家庭地址
            case "office_addr" => commonData.office_addr //  String, // 公司地址
            case "estate_purc_inte_ind" => commonData.estate_purc_inte_ind.toString //  Int, // 购房意向标志
            case "estate_fst_purc_inte_dt" => commonData.estate_fst_purc_inte_dt //  String, // 最近购房意向日期
            case "estate_purc_ind" => commonData.estate_purc_ind.toString //  Int, // 购房客户标志
            case "estate_purc_fst_dt" => commonData.estate_purc_fst_dt //  String, // 首次购房日期
            case "estate_owner_fst_dt" => commonData.estate_owner_fst_dt //  String, // 首次成为业主日期
            case "lodger_ind" => commonData.lodger_ind.toString //  Int, // 客房客户标志
            case "hotel_mem_ind" => commonData.hotel_mem_ind.toString //  Int, // 酒店会员标志
            case "hotel_fst_live_dt" => commonData.hotel_fst_live_dt //  String, // 酒店首次入住日期
            case "bevrg_cust_ind" => commonData.bevrg_cust_ind.toString //  Int, // 餐饮客户标志
            case "rest_mem_ind" => commonData.rest_mem_ind.toString //  Int, // 餐饮会员标志
            case "fst_bevrg_cust_dt" => commonData.fst_bevrg_cust_dt //  String, // 首次成为餐饮客户日期
            case "recrn_cust_ind" => commonData.recrn_cust_ind.toString //  Int, // 康乐会员标志
            case "fst_recrn_user_dt" => commonData.fst_recrn_user_dt //  String, // 首次成为康乐客户日期
            case "sport_mem_ind" => commonData.sport_mem_ind.toString //  Int, // 运动会员标志
            case "fst_mvmt_user_ind" => commonData.fst_mvmt_user_ind //  String, // 首次成为运动客户日期
            case "spring_cust_ind" => commonData.spring_cust_ind.toString //  Int, // 冰泉客户标志
            case "fst_o2o_cust_dt" => commonData.fst_o2o_cust_dt //  String, // 首次成为农牧O2O客户日期
            case "icing_qr_code_user_ind" => commonData.icing_qr_code_user_ind.toString //  Int, // 冰泉扫码用户标志
            case "fst_icing_qr_code_dt" => commonData.fst_icing_qr_code_dt //  String, // 首次扫码日期
            case "spring_scan_ind" => commonData.spring_scan_ind.toString //  Int, // 扫码注册会员标志
            case "pmal_mem_ind" => commonData.pmal_mem_ind.toString //  Int, // 积分商城会员标志
            case "sport_user_ind" => commonData.sport_user_ind.toString //  Int, // 体育客户标志
            case "fst_sport_user_dt" => commonData.fst_sport_user_dt //  String, // 首次成为体育客户日期
            case "foot_comn_usr_ind" => commonData.foot_comn_usr_ind.toString //  Int, // 足球普通用户标志
            case "foot_comn_usr_fst_dt" => commonData.foot_comn_usr_fst_dt //  String, // 足球普通用户注册日期
            case "annu_tick_usr_ind" => commonData.annu_tick_usr_ind.toString //  Int, // 足球套票用户标志
            case "annu_tick_usr_fst_dt" => commonData.annu_tick_usr_fst_dt //  String, // 足球套票用户注册日期
            case "evgrd_wld_cust_ind" => commonData.evgrd_wld_cust_ind.toString //  Int, // 恒大世界注册用户标志
            case "hd_fax_usr_ind" => commonData.hd_fax_usr_ind.toString //  Int, // 恒大金服注册用户标志
            case "hd_fax_usr_fst_dt" => commonData.hd_fax_usr_fst_dt //  String, // 金服注册日期
            case "data_dt" => commonData.data_dt //  String // 数据日期
            case _ => ""
          })
      indexList.filter(x => x._2.toString.length > 0).toMap
    }).filter(_._2.nonEmpty)

    commonIndex
  }


  def produceFlagIndex(): RDD[(Long, Map[String, String])] = {

    val flattenedTransformRulesForFlagIndex = flattenTransformRulesForFlagIndex().filter(x => x._3.equals(FlatConfig.inds_cls_cd))
    if (TransformerConfigure.isDebug) {
      println("Number of flag flattened rules = " + flattenedTransformRulesForFlagIndex.size)
      for (r <- flattenedTransformRulesForFlagIndex)
        println(r)
    }

    if (flattenedTransformRulesForFlagIndex.isEmpty) {
      return sc.emptyRDD
    }

    FlatConfig.inds_cls_cd match {
      case "1100" => // Estate
        val estateIndexData0 = DataObtain.getIntegratedEstateIndexFromHive(sc).map(x => (x.gu_indv_id, x))
        if (TransformerConfigure.isDebug) {
          println("Number of partitions for flag index data is: " + estateIndexData0.partitions.length)
        }

        val estateIndexData = {
          if (estateIndexData0.partitions.length > numPartitions * 4 || estateIndexData0.partitions.length < numPartitions / 2) {
            if (TransformerConfigure.isDebug) {
              println("Repartition flag index data from " +
                estateIndexData0.partitions.length + " partitions to " + numPartitions + " partitions.")
            }
            estateIndexData0.repartition(numPartitions).cache()
          } else
            estateIndexData0.cache()
        }

        if (TransformerConfigure.isDebug) {
          println("Number of flag index data = " + estateIndexData.count())
          estateIndexData.take(100).foreach(println)
        }

        val estateIndex = estateIndexData.mapValues(estateData => {
          val indexList = for ((idxTableName, idxColName, idxIndusClsCode, idxCalcMode) <- flattenedTransformRulesForFlagIndex)
            yield idxTableName + "." + idxColName ->
              (idxColName match {
                case "gu_indv_id" => estateData.gu_indv_id.toString //  String, // 个人客户统一编号
                case "gender_cd" => estateData.gender_cd //  String, // 性别
                case "age" => estateData.age.toString //  Int, // 年龄
                case "buy_hous_ind" => estateData.buy_hous_ind.toString //  Int, // 购房用户标志
                case "hous_nums" => estateData.hous_nums.toString //  Int, // 客户总购房套数
                case "max_bld_area" => estateData.max_bld_area.toString //  BigDecimal, // 建筑最大面积
                case "hous_value" => estateData.hous_value.toString //  BigDecimal, // 房产总价值
                case "estt_loan_typ" => estateData.estt_loan_typ //  String, // 贷款类型
                case "aj_loan_totl_amt" => estateData.aj_loan_totl_amt.toString //  BigDecimal, // 按揭贷款总额
                case "gjj_loan_totl_amt" => estateData.gjj_loan_totl_amt.toString //  BigDecimal, // 公积金贷款总额
                case "aj_loan_max_year" => estateData.aj_loan_max_year.toString //  Int, // 按揭贷款最长年限
                case "gjj_loan_max_year" => estateData.gjj_loan_max_year.toString //  Int, // 公积金贷款最长年限
                case "gjj_loan_pct" => estateData.gjj_loan_pct.toString //  BigDecimal, // 公积金贷款金额占比
                case "min_contr_dt" => estateData.min_contr_dt //  String, // 最早合同日期
                case "min_hdin_dt" => estateData.min_hdin_dt //  String, // 最早交房日期
                case "sell_prog_num" => estateData.sell_prog_num.toString //  Int, // 销售过程次数
                case "bef_contr_touch_ind" => estateData.bef_contr_touch_ind.toString //  Int, // 签合同前是否有接触
                case "contr_cmfm_opport_ind" => estateData.contr_cmfm_opport_ind.toString //  Int, // 合同是否来源于机会
                case "salestage" => estateData.salestage.toString //  Int, // 最终购房阶段
                case "istgtherbuyhou" => estateData.istgtherbuyhou.toString //  Int, // 是否两人共买一套
                case "bproducttypecode" => estateData.bproducttypecode //  String, // 房产类型
                case "roomstru" => estateData.roomstru //  String, // 房型
                case "data_dt" => estateData.data_dt //  String // 数据日期
                case _ => ""
              })
          indexList.filter(x => x._2.toString.length > 0).toMap
        })

        estateIndex.filter(x => x._2.nonEmpty)

      case "3110" => // Soccer
        //        val soccerIndex = TestData.getSoccerIndexData(sc).map(x => (x.Gu_Indv_Id, x))

        val soccerIndexData0 = DataObtain.getIntegratedSoccerIndexFromHive(sc).map(x => (x.Gu_Indv_Id, x))
        if (TransformerConfigure.isDebug) {
          println("Number of partitions for flag index data is: " + soccerIndexData0.partitions.length)
        }

        val soccerIndexData = {
          if (soccerIndexData0.partitions.length > numPartitions * 2 || soccerIndexData0.partitions.length < numPartitions / 2) {
            if (TransformerConfigure.isDebug) {
              println("Repartition flag index data from " +
                soccerIndexData0.partitions.length + " partitions to " + numPartitions + " partitions.")
            }
            soccerIndexData0.repartition(numPartitions).cache()
          } else
            soccerIndexData0.cache()
        }

        if (TransformerConfigure.isDebug) {
          println("Number of flag index data = " + soccerIndexData.count())
          soccerIndexData.take(100).foreach(println)
        }

        val soccerIndex = soccerIndexData.mapValues(soccerData => {
          val indexList = for ((idxTableName, idxColName, idxIndusClsCode, idxCalcMode) <- flattenedTransformRulesForFlagIndex)
            yield idxTableName + "." + idxColName ->
              (idxColName match {
                case "fst_buy_sig_tkt_dt" => soccerData.Fst_Buy_Sig_Tkt_Dt //首次购买单场票时间
                case "lat_buy_sig_tkt_dt" => soccerData.Lat_Buy_Sig_Tkt_Dt // String  //最近一次购买单场票时间
                case "fst_buy_vip_tkt_dt" => soccerData.Fst_Buy_Vip_Tkt_Dt // String  //首次购买套票时间
                case "lat_buy_vip_tkt_dt" => soccerData.Lat_Buy_Vip_Tkt_Dt // String  //最近一次购买套票时间
                case "fst_buy_shirt_dt" => soccerData.Fst_Buy_Shirt_Dt // String  //首次购买球衣时间
                case "lat_buy_shirt_dt" => soccerData.Lat_Buy_Shirt_Dt // String  //最近一次购买球衣时间
                case "fst_buy_souvenirs_dt" => soccerData.Fst_Buy_Souvenirs_Dt // String  //首次购买纪念品时间
                case "lat_buy_souvenirs_dt" => soccerData.Lat_Buy_Souvenirs_Dt // String  //最近一次购买纪念品时间
                case "sig_reg_user_ind" => soccerData.sig_reg_user_ind.toString // Int  //是否单场注册用户
                case "vip_reg_user_ind" => soccerData.vip_reg_user_ind.toString // Int  //是否套票注册用户
                case "buy_sig_tkt_ind" => soccerData.buy_sig_tkt_ind.toString // Int  //是否购买过单场票
                case "buy_vip_tkt_ind" => soccerData.buy_vip_tkt_ind.toString // Int  //是否购买过套票
                case "buy_shirt_ind" => soccerData.buy_shirt_ind.toString // Int  //是否购买过球衣
                case "buy_souvenirs_ind" => soccerData.buy_souvenirs_ind.toString // Int  //是否购买过纪念品
                case "cur_vip_score" => soccerData.Cur_Vip_Score.toString // Int  //当前积分
                case "used_vip_score" => soccerData.Used_Vip_Score.toString // Int  //已用积分
                case "accm_vip_score" => soccerData.Accm_Vip_Score.toString // Int  //累计积分
                case "com_recb_addr" => soccerData.Com_Recb_Addr // String  //常用收货地址
                case _ => ""
              })
          indexList.filter(x => x._2.toString.length > 0).toMap
        })

        soccerIndex.filter(x => x._2.nonEmpty)

      case "2000" =>
        val hotelIndexData0 = DataObtain.getIntegratedHotelIndexFromHive(sc).map(x => (x.gu_indv_id, x))

        if (TransformerConfigure.isDebug) {
          println("Number of partitions for flag index data is: " + hotelIndexData0.partitions.length)
        }

        val hotelIndexData = {
          if (hotelIndexData0.partitions.length > numPartitions * 2 || hotelIndexData0.partitions.length < numPartitions / 2) {
            if (TransformerConfigure.isDebug) {
              println("Repartition flag index data from " +
                hotelIndexData0.partitions.length + " partitions to " + numPartitions + " partitions.")
            }
            hotelIndexData0.repartition(numPartitions).cache()
          } else
            hotelIndexData0.cache()
        }

        if (TransformerConfigure.isDebug) {
          println("Number of flag index data = " + hotelIndexData.count())
          hotelIndexData.take(100).foreach(println)
        }

        val hotelIndex = hotelIndexData.mapValues(hotelData => {
          val indexList = for ((idxTableName, idxColName, idxIndusClsCode, idxCalcMode) <- flattenedTransformRulesForFlagIndex)
            yield idxTableName + "." + idxColName ->
              (idxColName match {
                case "gu_indv_id" => hotelData.gu_indv_id.toString // string,
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

  def produceFinalIndex(numIndexRDD: RDD[(Long, Map[String, String])],
                        flagIndexRDD: RDD[(Long, Map[String, String])],
                        commonIndexRDD: RDD[(Long, Map[String, String])])
  : RDD[(Long, Map[String, String])] = {

    //    numIndexRDD.cogroup(flagIndexRDD).mapValues(x => {
    //      var finalMap = Map[String, String]()
    //      for (index <- x._1)
    //        finalMap = finalMap ++ index
    //      for (index <- x._2)
    //        finalMap = finalMap ++ index
    //
    //      finalMap
    //    })

    numIndexRDD.fullOuterJoin(flagIndexRDD).mapValues {
      case (Some(m1), Some(m2)) => m1 ++ m2
      case (None, Some(m)) => m
      case (Some(m), None) => m
      case _ => Map("Error" -> "Wrong result from full outer join!")
    }.fullOuterJoin(commonIndexRDD).mapValues {
      case (Some(m1), Some(m2)) => m1 ++ m2
      case (None, Some(m)) => m
      case (Some(m), None) => m
      case _ => Map("Error" -> "Wrong result from full outer join!")
    }
  }

  // Convert the user metrics to discrete value according to defined rules.
  def produceDiscreteIndex(originalIndex: RDD[(Long, Map[String, String])])
  : RDD[(Long, Map[String, String])] = {

    val dspsRules = FlatConfig.getDspsRule().map(
      x => x.flat_clmn_nm.toLowerCase ->(x.dsps_alg_type_cd, x.dsps_rules.split(','), x.tag_ctgy_id.toString)).toMap

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

    val discreteIndex = originalIndex.mapValues(indexMap => {
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
              val idxValueNum = BigDecimal(indexMap(idx))
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
              val idxValueNum = BigDecimal(indexMap(idx))
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
        Map[String, String]()
    }).filter(_._2.nonEmpty).cache()

    if (TransformerConfigure.isDebug) {
      println("Records failed to be discretized: ")
      discreteIndex
        .flatMap(r =>
          for (i <- r._2; if i._2.startsWith("Missing") || i._2.startsWith("Unknown")) yield i
        ).aggregateByKey(Set[String]())((u, v) => u + v, (u1, u2) => u1 ++ u2)
        .collect().foreach(println)
    }

    println("Sum of tags:")
    discreteIndex
      .flatMap(r => for (i <- r._2) yield (i._2, 1))
      .reduceByKey(_ + _)
      .collect().sortBy(_._1).foreach(println)

    discreteIndex
  }

  def export2Redis(finalResult: RDD[(Long, Map[String, String])]): Unit = {
    if (TransformerConfigure.isDebug) {
      println("Number of partitions in RDD to be exported to Redis is " + finalResult.partitions.length)
    }

    val rddToExport = {
      if (finalResult.partitions.length > 32) {
        if (TransformerConfigure.isDebug)
          println("Repartition RDD to be exported to 16 partitions.")

        finalResult.coalesce(16)
      } else
        finalResult
    }.cache()

    val metricsCount2Redis = sc.accumulator(0)
    val custCount2Redis = sc.accumulator(0)

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
              pipeline.setbit(redisKey, row._1, true)
              i += 1
              metricsCount2Redis += 1
              if (i == 1000000) {
                pipeline.sync()
                pipeline = jedis.pipelined()
                i = 0
              }
            }
            custCount2Redis += 1
          })

          pipeline.sync()
          RedisOperUtil.returnResource(jedis)
        }
      } catch {
        case e: Exception =>
          println("Exception raised in exporting tags to Redis.")
      }
    })

    println("Totally " + custCount2Redis.value + " customers with "
      + metricsCount2Redis.value + " metrics written to Redis.")
  }

  def export2HBase(tableName: String, finalResult: RDD[(Long, Map[String, String])]): Unit = {

    //    if (TransformerConfigure.isDebug) {
    //      val badRecordSet = finalResult.filter(x => x._1 == null || x._2 == null || x._2.isEmpty)
    //      val badCount = badRecordSet.count()
    //      if (badCount > 0) {
    //        println("There are " + badCount + " metrics records have empty field set, write to HBase is canceled.")
    //        val badCount1 = badRecordSet.filter(x => x._1 == null).count()
    //        val badCount2 = badRecordSet.filter(x => x._2 == null).count()
    //        val badCount3 = badRecordSet.filter(x => x._2.isEmpty).count()
    //        println("BadCount1=" + badCount1 + ", BadCount2=" + badCount2 + ", BadCount3=" + badCount3)
    //        if (badCount1 > 0) {
    //          println("Bad1")
    //          badRecordSet.filter(x => x._1 == null).collect().foreach(println)
    //        }
    //        if (badCount2 > 0) {
    //          println("Bad2")
    //          badRecordSet.filter(x => x._2 == null).collect().foreach(println)
    //        }
    //        if (badCount3 > 0) {
    //          println("Bad3")
    //          badRecordSet.filter(x => x._2.isEmpty).collect().foreach(println)
    //        }
    //        return
    //      }
    //    }


    if (TransformerConfigure.isDebug) {
      println("Number of partitions in RDD to be exported to HBase is " + finalResult.partitions.length)
    }

    val rddToExport = {
      if (finalResult.partitions.length > 32) {
        if (TransformerConfigure.isDebug)
          println("Repartition RDD to be exported to 16 partitions.")

        finalResult.coalesce(16)
      } else
        finalResult
    }.cache()

    val metricsCount2HBase = sc.accumulator(0)
    val custCount2HBase = sc.accumulator(0)

    // Write to HBase
    rddToExport.foreachPartition(x => {
      val myConf = HBaseConfiguration.create()
      val myTable = new HTable(myConf, TableName.valueOf(tableName))
      myTable.setAutoFlush(false, false)
      myTable.setWriteBufferSize(64 * 1024 * 1024)
      x.foreach(a => {

        val p = new Put(Bytes.toBytes(a._1))

        val indices = a._2
        if (indices.nonEmpty) {
          indices.foreach(b => {
            p.add("cf".getBytes, Bytes.toBytes(b._1), Bytes.toBytes(b._2))
            metricsCount2HBase += 1
          })
          myTable.put(p)
        } else {
          println("WARNING!!! No metrics found for " + a._1.toString)
        }

        custCount2HBase += 1
      })
      myTable.flushCommits()
    })

    println("Totally " + custCount2HBase.value + " customers with "
      + metricsCount2HBase.value + " metrics written to HBase.")

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
    val indexDate = dateFormat.parse(customerProdGrpIndexData.Statt_Dt)
    val indexCalcMode = indexRule._2

    val targetIndexName = "metric" + "." +
      customerProdGrpIndexData.Prod_Grp_ID.toLowerCase + "." +
      customerProdGrpIndexData.Statt_Indx_ID.toLowerCase + "." +
      indexRule._1 + "." + (
      indexCalcMode match {
        case "10" => "current"
        case "20" => "sum7d"
        case "21" => "sum30d"
        case _ => "unknown"
      })

    val indexValue: BigDecimal = indexRule._1 match {
      case "day_indx_val" => customerProdGrpIndexData.Day_Indx_Val
      case "month_indx_val" => customerProdGrpIndexData.Month_Indx_Val
      case "quarter_indx_val" => customerProdGrpIndexData.Quarter_Indx_val // Note: the 'v' is lower case.
      case "year_indx_val" => customerProdGrpIndexData.Year_Indx_Val
      case "accm_indx_val" => customerProdGrpIndexData.Accm_Indx_Val
      case "rec_60d_indx_val" => customerProdGrpIndexData.Rec_60d_Indx_Val
      case "rec_90d_indx_val" => customerProdGrpIndexData.Rec_90d_Indx_Val
      case "rec_180d_indx_val" => customerProdGrpIndexData.Rec_180d_Indx_Val
      case "rec_360d_indx_val" => customerProdGrpIndexData.Rec_360d_Indx_Val
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
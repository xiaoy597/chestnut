package com.hd.bigdata

import java.awt.Polygon
import java.text.SimpleDateFormat
import java.util.Calendar

import com.hd.bigdata.utils.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by lingxiao on 2016/5/10.
  */


object DataObtain {
  def getSqlForNumIndexData(industryClassCode: String): String = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val edt = sdf.format(DateUtils.today)
    var sdt = ""

    var sql = ""
    var stattIndxId = ""
    var dimId = ""
    var indxCalcMode = ""

    sql += "SELECT " +
      "Statt_Dt,\nStatt_Dt_Typ_Cd,\nGu_Indv_Id,\nProd_Grp_ID,\nStatt_Indx_ID,\nDay_Indx_Val,\nMonth_Indx_Val,\nQuarter_Indx_val,\nYear_Indx_Val,\nAccm_Indx_Val,\nData_Dt,\nRec_60d_Indx_Val,\nRec_90d_Indx_Val,\nRec_180d_Indx_Val,\nRec_360d_Indx_Val,\nInds_Cls_Cd "


    sql = sql.substring(0, sql.length - 1)
    sql += ("\nFROM " + "h52_inds_statt_indx_rslt_g" + " \nWHERE ")

    val listRule = FlatConfig.getRuleConditions(industryClassCode)

    if (listRule.isEmpty)
      sql += "1 = 0"
    else {
      for (i <- listRule.indices) {
        stattIndxId = listRule(i).statt_indx_id
        dimId = listRule(i).dim_id
        //返回数据格式为：20100002,20001, 21,20,10 head为最大的计算方式（30天）
        if (listRule(i).indx_calc_mode_cd != null) {
          indxCalcMode = listRule(i).indx_calc_mode_cd.split(",").head

          //汇总-最近30天
          if (indxCalcMode == "21") {
            sdt = sdf.format(DateUtils.getDateBeforeDays(30))

            //汇总-最近7天
          } else if (indxCalcMode == "20") {
            sdt = sdf.format(DateUtils.getDateBeforeDays(7))
          } else {
            sdt = sdf.format(DateUtils.getDateBeforeDays(1))
          }

        }
        //            System.out.println(stattIndxId + "," + dimId + "," + indxCalcMode + "," + sdt+ "," + edt)

        sql += "(Statt_Indx_ID = '" + stattIndxId + "' and Prod_Grp_ID = '" + dimId + "' and Data_dt <= '" + edt + "' and Data_dt > '" + sdt + "' ) or \n"

      }
      sql = sql.substring(0, sql.length - 4)
      //          sql = sql + "\nLIMIT 10"
    }

    sql
  }

  /**
    * 获取查询SQL
    *
    * @param industryClassCode
    * @return
    */
  def getSrcDataSql(industryClassCode: String): String = {

    //获取当前表的字段信息
    //    val list = FlatConfig.getTableColInfo(null).filter( i => i.indx_tbl_nm == tableName)
    val listCol = FlatConfig.getTableColInfo(industryClassCode)
    val listFlatMode = FlatConfig.getFlatMode(industryClassCode)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val edt = sdf.format(DateUtils.today)
    var sdt = ""

    var sql = ""
    var stattIndxId = ""
    var dimId = ""
    var indxCalcMode = ""

    if (listCol.length > 0) {

      if (listFlatMode.length == 1) {

        if (listFlatMode.head.flat_mode_cd == "20") {
          //行转列

          sql += "SELECT " +
            "Statt_Dt,\nStatt_Dt_Typ_Cd,\nGu_Indv_Id,\nProd_Grp_ID,\nStatt_Indx_ID,\nDay_Indx_Val,\nMonth_Indx_Val,\nQuarter_Indx_val,\nYear_Indx_Val,\nAccm_Indx_Val,\nData_Dt,\nRec_60d_Indx_Val,\nRec_90d_Indx_Val,\nRec_180d_Indx_Val,\nRec_360d_Indx_Val,\nInds_Cls_Cd "

          //          for (i <- 0 until listCol.length)
          //            sql += listCol(i).indx_clmn_nm + " ,"

          sql = sql.substring(0, sql.length - 1)
          sql += ("\nFROM " + industryClassCode + " \nWHERE ")

          val listRule = FlatConfig.getRuleConditions(industryClassCode)
          for (i <- 0 until listRule.length) {
            stattIndxId = listRule(i).statt_indx_id
            dimId = listRule(i).dim_id
            //返回数据格式为：20100002,20001, 21,20,10 head为最大的计算方式（30天）
            if (listRule(i).indx_calc_mode_cd != null) {
              indxCalcMode = listRule(i).indx_calc_mode_cd.split(",").head

              //汇总-最近30天
              if (indxCalcMode == "21") {
                sdt = sdf.format(DateUtils.getDateBeforeDays(30))

                //汇总-最近7天
              } else if (indxCalcMode == "20") {
                sdt = sdf.format(DateUtils.getDateBeforeDays(7))
              } else {
                sdt = sdf.format(DateUtils.getDateBeforeDays(1))
              }

            }
            //            System.out.println(stattIndxId + "," + dimId + "," + indxCalcMode + "," + sdt+ "," + edt)

            sql += "(Statt_Indx_ID = '" + stattIndxId + "' and Prod_Grp_ID = '" + dimId + "' and Data_dt <= '" + edt + "' and Data_dt > '" + sdt + "' ) or \n"

          }
          sql = sql.substring(0, sql.length - 4)
          //          sql = sql + "\nLIMIT 10"

        } else {
          //直接扁平化
          System.out.println("直接扁平化。")
        }
      } else {
        System.out.println("源表扁平化方式不唯一/没有扁平化方式。")
      }


    }
    sql
  }

  def getIntegratedSoccerIndexFromHive(sc : SparkContext) : RDD[SoccerIndexData] = {
    val sqlContext = new HiveContext(sc)

    import sqlContext.implicits._
//    sqlContext.sql("use csum");

    val getSrcDataStmt = "SELECT * FROM csum.h52_ftb_cust_integrate_info_g where Gu_Indv_Id is not null"
    println(getSrcDataStmt)

    val dataFrame = sqlContext.sql(getSrcDataStmt);

    println("Sample Data: ")
    dataFrame.show(10)

    println("Number of rows returned : " + dataFrame.count())

    //下面取
    val dataRDD = dataFrame.map(data => {
      //System.out.println(data.getString(0),data.getString(1),data.getString(2))
      SoccerIndexData(
        if (data.isNullAt(0)) "" else data.getString(0),
        if (data.isNullAt(1)) "" else data.getString(1),
        if (data.isNullAt(2)) "" else data.getString(2),
        if (data.isNullAt(3)) "" else data.getString(3),
        if (data.isNullAt(4)) "" else data.getString(4),
        if (data.isNullAt(5)) "" else data.getString(5),
        if (data.isNullAt(6)) "" else data.getString(6),
        if (data.isNullAt(7)) "" else data.getString(7),
        if (data.isNullAt(8)) "" else data.getString(8),

        if (data.isNullAt(9)) 0 else data.getInt(9),
        if (data.isNullAt(10)) 0 else data.getInt(10),
        if (data.isNullAt(11)) 0 else data.getInt(11),
        if (data.isNullAt(12)) 0 else data.getInt(12),
        if (data.isNullAt(13)) 0 else data.getInt(13),
        if (data.isNullAt(14)) 0 else data.getInt(14),

        if (data.isNullAt(15)) 0 else data.getDecimal(15),
        if (data.isNullAt(16)) 0 else data.getDecimal(16),
        if (data.isNullAt(17)) 0 else data.getDecimal(17),

        if (data.isNullAt(18)) "" else data.getString(18),
        if (data.isNullAt(19)) "" else data.getString(19))
    })

    dataRDD
  }

  def getIntegratedHotelIndexFromHive(sc : SparkContext) : RDD[HotelIndexData] = {
    val sqlContext = new HiveContext(sc)

    import sqlContext.implicits._
    //    sqlContext.sql("use csum");

    val getSrcDataStmt = "SELECT * FROM csum.h52_hotel_unif_cust_csum where gu_indv_id is not null"
    println(getSrcDataStmt)

    val dataFrame = sqlContext.sql(getSrcDataStmt);

    println("Sample Data: ")
    dataFrame.show(10)

    println("Number of rows returned : " + dataFrame.count())

    //下面取
    val dataRDD = dataFrame.map(data => {
      //System.out.println(data.getString(0),data.getString(1),data.getString(2))
      new HotelIndexData(
        if (data.isNullAt(0)) "" else data.getString(0),

        if (data.isNullAt(1)) 0 else data.getShort(1),
        if (data.isNullAt(2)) 0 else data.getShort(2),
        if (data.isNullAt(3)) 0 else data.getShort(3),
        if (data.isNullAt(4)) 0 else data.getShort(4),
        if (data.isNullAt(5)) 0 else data.getShort(5),
        if (data.isNullAt(6)) 0 else data.getShort(6),
        if (data.isNullAt(7)) 0 else data.getInt(7),
        if (data.isNullAt(8)) 0 else data.getInt(8),
        if (data.isNullAt(9)) 0 else data.getInt(9),

        if (data.isNullAt(10)) 0 else data.getDecimal(10),
        if (data.isNullAt(11)) 0 else data.getDecimal(11),
        if (data.isNullAt(12)) 0 else data.getDecimal(12),

        if (data.isNullAt(13)) 0 else data.getInt(13),
        if (data.isNullAt(14)) 0 else data.getInt(14),
        if (data.isNullAt(15)) 0 else data.getInt(15),
        if (data.isNullAt(16)) 0 else data.getInt(16),
        if (data.isNullAt(17)) 0 else data.getInt(17),
        if (data.isNullAt(18)) 0 else data.getInt(18),
        if (data.isNullAt(19)) 0 else data.getInt(19),
        if (data.isNullAt(20)) 0 else data.getInt(20),

        if (data.isNullAt(21)) 0 else data.getDecimal(21),
        if (data.isNullAt(22)) 0 else data.getDecimal(22),
        if (data.isNullAt(23)) 0 else data.getDecimal(23),
        if (data.isNullAt(24)) 0 else data.getDecimal(24),
        if (data.isNullAt(25)) 0 else data.getDecimal(25),
        if (data.isNullAt(26)) 0 else data.getDecimal(26),
        if (data.isNullAt(27)) 0 else data.getDecimal(27),
        if (data.isNullAt(28)) 0 else data.getDecimal(28),
        if (data.isNullAt(29)) 0 else data.getDecimal(29),
        if (data.isNullAt(30)) 0 else data.getDecimal(30),
        if (data.isNullAt(31)) 0 else data.getDecimal(31),
        if (data.isNullAt(32)) 0 else data.getDecimal(32),

        if (data.isNullAt(33)) 0 else data.getInt(33),

        if (data.isNullAt(34)) "" else data.getString(34),
        if (data.isNullAt(35)) "" else data.getString(35),

        if (data.isNullAt(36)) 0 else data.getInt(36),
        if (data.isNullAt(37)) 0 else data.getInt(37),
        if (data.isNullAt(38)) 0 else data.getInt(38),

        if (data.isNullAt(39)) "" else data.getString(39),
        if (data.isNullAt(40)) "" else data.getString(40),
        if (data.isNullAt(41)) "" else data.getString(41),
        if (data.isNullAt(42)) "" else data.getString(42),

        if (data.isNullAt(43)) 0 else data.getInt(43),
        if (data.isNullAt(44)) 0 else data.getInt(44),
        if (data.isNullAt(45)) 0 else data.getInt(45),

        if (data.isNullAt(46)) "" else data.getString(46),
        if (data.isNullAt(47)) "" else data.getString(47),
        if (data.isNullAt(48)) "" else data.getString(48))

    })

    dataRDD
  }

  /**
    * 执行hive
    *
    * @param industryClassCode
    * @return
    */
  def getIntegratedNumIndexFromHive(industryClassCode: String, sc: SparkContext): RDD[CustomerProdGrpIndexData] = {

    val sqlContext = new HiveContext(sc)

    import sqlContext.implicits._
    sqlContext.sql("use csum");

    val getSrcDataStmt = getSqlForNumIndexData(industryClassCode)
    println(getSrcDataStmt)

    val dataFrame = sqlContext.sql(getSrcDataStmt);

    println("Sample Data: ")
    dataFrame.show(10)

    println("Number of rows returned : " + dataFrame.count())

    //下面取
    val dataRDD = dataFrame.map(data => {
      //System.out.println(data.getString(0),data.getString(1),data.getString(2))
      CustomerProdGrpIndexData(
        data.getString(0),
        data.getString(1),
        data.getString(2),
        data.getString(3),
        data.getString(4),

        if (data.isNullAt(5)) 0 else data.getDecimal(5),
        if (data.isNullAt(6)) 0 else data.getDecimal(6),
        if (data.isNullAt(7)) 0 else data.getDecimal(7),
        if (data.isNullAt(8)) 0 else data.getDecimal(8),
        if (data.isNullAt(9)) 0 else data.getDecimal(9),

        if (data.isNullAt(10)) "" else data.getString(10),
        if (data.isNullAt(11)) 0 else data.getDecimal(11),
        if (data.isNullAt(12)) 0 else data.getDecimal(12),
        if (data.isNullAt(13)) 0 else data.getDecimal(13),
        if (data.isNullAt(14)) 0 else data.getDecimal(14),
        if (data.isNullAt(15)) "" else data.getString(15))
    })
    //      .collect()

    //    dataRDD.toDF().show(10)
    dataRDD

  }

  //Usage: DataObtain <industry_class_code> <today:yyyy-MM-dd>
  def main(args: Array[String]): Unit = {
    //"h52_inds_statt_indx_rslt_g"

    //    System.out.println(getSrcDataSql(args(0)))
    var conf = new SparkConf().setAppName("wordcount")

    val sc = new SparkContext(conf)

    DateUtils.today = new SimpleDateFormat("yyyy-MM-dd").parse(args(1))

    getIntegratedNumIndexFromHive(args(0), sc)

  }
}

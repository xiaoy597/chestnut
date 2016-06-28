package com.hd.bigdata

import java.sql.{ResultSet, DriverManager, PreparedStatement, Connection}

import com.hd.bigdata.utils.JDBCUtils
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
  * Created by lingxiao on 2016/5/9.
  */

case class FlatRuleConfig(
                           val indx_tbl_nm: String, //指标表名称
                           val inds_cls_cd: String, // 产业分类代码
                           var indx_clmn_nm: String, //指标表字段名称
                           var statt_indx_id: String, //统计指标编号
                           var dim_id: String, //维度ID
                           var indx_calc_mode_cd: String, //指标计算方式代码
                           var indx_calc_mode_nm: String, //指标计算方式名称 10：直接取数 20：汇总-最近7天 21：汇总-最近30天 99：未加工
                           var flat_tbl_nm: String, //扁平表名称
                           var flat_clmn_nm: String, //扁平表字段名称 自动生成，根据扁平化方式不同 直接：指标表表名+指标表字段名 行转列：维度ID+指标ID+度量字段名
                           var active_ind: String //有效标志 1：有效；0：无效
                         )

case class TableColInfo(
                         var indx_tbl_nm: String, //指标表名称
                         var indx_tbl_desc: String, //指标表描述
                         var inds_cls_cd: String, //产业分类代码
                         var indx_clmn_nm: String, //指标表字段名称
                         var indx_clmn_desc: String, //指标表字段描述
                         var msmt_clmn_ind: String //度量字段标识  1:度量 0:非度量
                       )

case class FlatMode(
                     var indx_tbl_nm: String, //指标表名称
                     var indx_tbl_desc: String, //指标表描述
                     var inds_cls_cd: String, //产业分类代码
                     var flat_mode_cd: String, //扁平化方式代码
                     var flat_mode_nm: String //扁平化方式名称 10：直接扁平化 20：行转列

                   )

case class RuleCondition(
                          var statt_indx_id: String, //统计指标编号
                          var dim_id: String, //维度ID
                          var indx_calc_mode_cd: String //指标计算方式代码
                        )

case class DspsRule(
                     flat_tbl_nm: String,
                     flat_clmn_nm: String,
                     tag_ctgy_id: Int,
                     dsps_alg_id: String,
                     dsps_alg_type_cd: String,
                     dsps_rules: String
                   )

case class DspsMappingPara(
                            dsps_mapping_para_class: String,
                            dsps_mapping_para_key: String,
                            dsps_mapping_para_value: String
                          )


object FlatConfig {

  var conn: Connection = null
  var ps: PreparedStatement = null
  var rs: ResultSet = null

  def getDspsMappingPara(): List[DspsMappingPara] = {

    val itemList = ListBuffer[DspsMappingPara]()

    val sql = "SELECT * from h50_dsps_mapping_para"

    println("SQL for getting dsps mapping parameters : [" + sql + "]")

    try {
      conn = JDBCUtils.getConn()

      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery

      while (rs.next) {
        itemList += DspsMappingPara(
          rs.getString("dsps_mapping_para_class"),
          rs.getString("dsps_mapping_para_key"),
          rs.getString("dsps_mapping_para_value"))
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      JDBCUtils.closeConn(rs, ps, conn)
    }

    itemList.toList
  }


  def getDspsRule(): List[DspsRule] = {

    val itemList = ListBuffer[DspsRule]()

    val sql = "SELECT t1.flat_tbl_nm, t1.flat_clmn_nm, t1.tag_ctgy_id, t1.dsps_alg_id, t2.dsps_alg_type_cd , " +
      " group_concat(t3.rule_para_value order by t3.rule_para_seq) as dsps_rules " +
      "FROM h50_dsps_tag_mapping t1 " +
      "join h50_dsps_alg_info t2 on t1.dsps_alg_id = t2.dsps_alg_id " +
      "join h50_dsps_alg_rule_para t3 on t1.dsps_alg_id = t3.dsps_alg_id " +
      "GROUP BY t1.flat_tbl_nm, t1.flat_clmn_nm, t1.tag_ctgy_id, t1.dsps_alg_id, t2.dsps_alg_type_cd"

    println("SQL for getting dsps rules: [" + sql + "]")

    try {
      conn = JDBCUtils.getConn()

      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery

      while (rs.next) {
        itemList += DspsRule(
          rs.getString("flat_tbl_nm"),
          rs.getString("flat_clmn_nm"),
          rs.getInt("tag_ctgy_id"),
          rs.getString("dsps_alg_id"),
          rs.getString("dsps_alg_type_cd"),
          rs.getString("dsps_rules"))
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      JDBCUtils.closeConn(rs, ps, conn)
    }
    itemList.toList
  }

  /**
    * 获取有效（active_ind=1）扁平化规则
    *
    * @return
    */
  def getFlatRuleConfig(tableName: String, industryClassCode: String): ListBuffer[FlatRuleConfig] = {

    var item: FlatRuleConfig = null
    val itemList = ListBuffer[FlatRuleConfig]()

    var sql = ""

    if (tableName == null) {
      sql = "SELECT t1.indx_tbl_nm , t1.inds_cls_cd, t1.indx_clmn_nm , t1.statt_indx_id , t1.dim_id , t1.indx_calc_mode_cd , t2.indx_calc_mode_nm , t1.flat_tbl_nm , t1.flat_clmn_nm , t1.active_ind " +
        "FROM h50_flat_rule_config t1 " +
        "JOIN h50_calc_mode t2 ON t1.indx_calc_mode_cd = t2.indx_calc_mode_cd " +
        "JOIN h50_indx_tbl_info t3 ON t1.indx_tbl_nm = t3.indx_tbl_nm " +
        "and t1.inds_cls_cd = t3.inds_cls_cd " +
        "WHERE t1.active_ind = 1 and (t3.inds_cls_cd = '' or t3.inds_cls_cd = '" + industryClassCode + "') "
    } else {
      sql = "SELECT t1.indx_tbl_nm , t1.inds_cls_cd, t1.indx_clmn_nm , t1.statt_indx_id , t1.dim_id , t1.indx_calc_mode_cd , t2.indx_calc_mode_nm , t1.flat_tbl_nm , t1.flat_clmn_nm , t1.active_ind " +
        "FROM h50_flat_rule_config t1 " +
        "JOIN h50_calc_mode t2 ON t1.indx_calc_mode_cd = t2.indx_calc_mode_cd " +
        "JOIN h50_indx_tbl_info t3 ON t1.indx_tbl_nm = t3.indx_tbl_nm " +
        "and t1.inds_cls_cd = t3.inds_cls_cd " +
        "WHERE t1.active_ind = 1 and t1.indx_tbl_nm = '" + tableName + "' " +
        "and (t3.inds_cls_cd = '' or t3.inds_cls_cd = '" + industryClassCode + "') "
    }

    try {
      conn = JDBCUtils.getConn()

      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery

      while (rs.next) {
        item = FlatRuleConfig(
          rs.getString("indx_tbl_nm").toLowerCase,
          rs.getString("inds_cls_cd"),
          rs.getString("indx_clmn_nm").toLowerCase,
          rs.getString("statt_indx_id"),
          rs.getString("dim_id"),
          rs.getString("indx_calc_mode_cd"),
          rs.getString("indx_calc_mode_nm"),
          rs.getString("flat_tbl_nm"),
          rs.getString("flat_clmn_nm"),
          rs.getString("active_ind"))
        itemList += item
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      JDBCUtils.closeConn(rs, ps, conn)
    }
    itemList
  }

  /**
    * 获取源表及字段信息
    *
    * @return
    */
  def getTableColInfo(industryClassCode: String): ListBuffer[TableColInfo] = {

    var item: TableColInfo = null
    val itemList = ListBuffer[TableColInfo]()
    var sql = ""
    if (industryClassCode == null) {
      sql = "SELECT t1.indx_tbl_nm, t1.indx_tbl_desc, t1.inds_cls_cd, t2.indx_clmn_nm, t2.indx_clmn_desc, t2.msmt_clmn_ind " +
        "FROM h50_indx_tbl_info t1 JOIN h50_indx_clmn_info t2 ON t1.indx_tbl_nm = t2.indx_tbl_nm " +
        "ORDER BY t1.indx_tbl_nm, t2.indx_clmn_nm "
    } else {
      sql = "SELECT t1.indx_tbl_nm, t1.indx_tbl_desc, t1.inds_cls_cd, t2.indx_clmn_nm, t2.indx_clmn_desc, t2.msmt_clmn_ind " +
        "FROM h50_indx_tbl_info t1 JOIN h50_indx_clmn_info t2 ON t1.indx_tbl_nm = t2.indx_tbl_nm " +
        "WHERE t1.inds_cls_cd = '" + industryClassCode + "' " +
        "ORDER BY t1.indx_tbl_nm, t2.indx_clmn_nm "
    }


    try {
      conn = JDBCUtils.getConn()

      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery

      while (rs.next) {
        item = TableColInfo(rs.getString("indx_tbl_nm"), rs.getString("indx_tbl_desc"), rs.getString("inds_cls_cd"), rs.getString("indx_clmn_nm")
          , rs.getString("indx_clmn_desc"), rs.getString("msmt_clmn_ind"))
        itemList += item
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      JDBCUtils.closeConn(rs, ps, conn)
    }
    itemList
  }

  /**
    * 获取源表扁平化方式
    *
    * @return
    */
  def getFlatMode(industryClassCode: String): ListBuffer[FlatMode] = {

    var item: FlatMode = null
    val itemList = ListBuffer[FlatMode]()

    var sql = ""
    if (industryClassCode == null) {
      sql = "SELECT t1.indx_tbl_nm, t1.indx_tbl_desc, t1.inds_cls_cd, t1.flat_mode_cd, t2.flat_mode_nm " +
        "FROM h50_indx_tbl_info t1 JOIN h50_flat_mode t2 ON t1.flat_mode_cd = t2.flat_mode_cd"
    } else {
      sql = "SELECT t1.indx_tbl_nm, t1.indx_tbl_desc, t1.inds_cls_cd, t1.flat_mode_cd, t2.flat_mode_nm " +
        "FROM h50_indx_tbl_info t1 JOIN h50_flat_mode t2 ON t1.flat_mode_cd = t2.flat_mode_cd " +
        "WHERE t1.inds_cls_cd = '" + industryClassCode + "' "
    }

    try {
      conn = JDBCUtils.getConn()

      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery

      while (rs.next) {
        item = FlatMode(rs.getString("indx_tbl_nm"), rs.getString("indx_tbl_desc"), rs.getString("inds_cls_cd"), rs.getString("flat_mode_cd")
          , rs.getString("flat_mode_nm"))
        itemList += item
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      JDBCUtils.closeConn(rs, ps, conn)
    }
    itemList
  }

  /**
    * 获取查询条件
    *
    * @param industryClassCode
    * @return
    */
  def getRuleConditions(industryClassCode: String): ListBuffer[RuleCondition] = {

    var item: RuleCondition = null
    val itemList = ListBuffer[RuleCondition]()

    var sql = ""

    if (industryClassCode == null) {
      sql = "SELECT t1.statt_indx_id , t1.dim_id , group_concat(t1.indx_calc_mode_cd order by t1.indx_calc_mode_cd  desc ) as indx_calc_mode_cd " +
        "FROM h50_flat_rule_config t1 join h50_indx_tbl_info t2 " +
        "on t1.indx_tbl_nm = t2.indx_tbl_nm and t1.inds_cls_cd = t2.inds_cls_cd " +
        "WHERE t1.active_ind = 1 " +
        "and t2.flat_mode_cd = '20' " +
        "GROUP BY t1.statt_indx_id , t1.dim_id"
    } else {
      sql = "SELECT t1.statt_indx_id , t1.dim_id , group_concat(t1.indx_calc_mode_cd order by t1.indx_calc_mode_cd  desc ) as indx_calc_mode_cd " +
        "FROM h50_flat_rule_config t1 join h50_indx_tbl_info t2 " +
        "on t1.indx_tbl_nm = t2.indx_tbl_nm and t1.inds_cls_cd = t2.inds_cls_cd " +
        "WHERE t1.active_ind = 1 AND t2.inds_cls_cd = '" + industryClassCode + "' " +
        "and t2.flat_mode_cd = '20' " +
        "GROUP BY t1.statt_indx_id , t1.dim_id"
    }

    println("SQL for getting index filter conditions: [" + sql + "]")

    try {
      conn = JDBCUtils.getConn()

      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery

      while (rs.next) {
        item = RuleCondition(rs.getString("statt_indx_id"), rs.getString("dim_id"), rs.getString("indx_calc_mode_cd"))
        itemList += item
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      JDBCUtils.closeConn(rs, ps, conn)
    }
    itemList
  }

  def main(args: Array[String]) {
    //    val conf = new SparkConf().setAppName("sparkToMysql").setMaster("local[4]")
    //    val sc = new SparkContext(conf)

    //    val list = getFlatRuleConfig("Hotel_common_table")
    //      val list = getTableColInfo("Hotel_common_table")
    //    val list = getFlatMode("Hotel_common_table")
    val list = getRuleConditions("Hotel_common_table")

    for (i <- 0 until list.length)
    //      System.out.println(list(i).indx_tbl_nm + "," + list(i).indx_clmn_nm + "," + list(i).statt_indx_id + "," + list(i).dim_id + "," + list(i).indx_calc_mode_cd + "," + list(i).indx_calc_mode_nm + "," + list(i).flat_tbl_nm + "," + list(i).flat_clmn_nm + "," + list(i).active_ind)
    //      System.out.println(list(i).indx_tbl_nm + "," + list(i).indx_tbl_desc + "," + list(i).inds_cls_cd + "," + list(i).indx_clmn_nm + "," + list(i).indx_clmn_desc + "," + list(i).msmt_clmn_ind)
    //      System.out.println(list(i).indx_tbl_nm + "," + list(i).indx_tbl_desc + "," + list(i).inds_cls_cd + "," + list(i).flat_mode_cd + "," + list(i).flat_mode_nm)
      System.out.println(list(i).statt_indx_id + "," + list(i).dim_id + "," + list(i).indx_calc_mode_cd)
  }
}

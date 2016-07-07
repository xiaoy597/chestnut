package com.evergrande.bigdata

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat

import com.evergrande.bigdata.utils.{JDBCUtils, DateUtils, TransformerConfigure}

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
                           var flat_mode_cd: String, // 扁平化方式代码
                           var dim_clmn_nm: String, // 维度字段名称
                           val key_clmn_nm: String, // 指标表键值字段
                           val ext_condition: String // 指标表查询条件
                         )

case class FlatTableConfig(
                            indx_tbl_nm: String, //指标表名称
                            inds_cls_cd: String, // 产业分类代码
                            flat_mode_cd: String, // 扁平化方式代码
                            dim_clmn_nm: String, // 维度字段名称
                            key_clmn_nm: String, // 指标表键值字段
                            ext_condition: String // 指标表查询条件
                          )

case class FlatColumnConfig(
                             indx_clmn_nm: String, //指标表字段名称
                             statt_indx_id: String, //统计指标编号
                             dim_id: String, //维度ID
                             indx_calc_mode_cd: String //指标计算方式代码
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

case class IndexCategory(
                          indx_cat_cd: String, // '指标体系代码',
                          indx_cat_nm: String, // '指标体系名称',
                          metrics_tbl_nm: String, // '指标宽表名称',
                          tag_tbl_nm: String // '标签宽表名称',
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
                     dsps_paras: String
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
  var indx_cat_cd: String = null
  var inds_cls_cd: String = null

  def getIndexCategory() : List[IndexCategory] = {
    val itemList = ListBuffer[IndexCategory]()

    val sql = "SELECT * from h50_indx_cat_info"

    println("SQL for getting index category info : [" + sql + "]")

    try {
      conn = JDBCUtils.getConn()

      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery

      while (rs.next) {
        itemList += IndexCategory(
          rs.getString("indx_cat_cd"),
          rs.getString("indx_cat_nm"),
          rs.getString("metrics_tbl_nm"),
          rs.getString("tag_tbl_nm"))
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      JDBCUtils.closeConn(rs, ps, conn)
    }

    itemList.toList
  }

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
      " group_concat(t3.rule_para_value order by t3.rule_para_seq) as dsps_paras " +
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
          rs.getString("dsps_paras"))
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
  def getFlatRuleConfig(): Map[FlatTableConfig, List[FlatColumnConfig]] = {

    var item: FlatRuleConfig = null
    val itemList = ListBuffer[FlatRuleConfig]()

    val sql = "SELECT t1.indx_tbl_nm , t1.inds_cls_cd, t1.indx_clmn_nm , " +
      "t1.statt_indx_id , t1.dim_id , t1.indx_calc_mode_cd , t2.indx_calc_mode_nm , " +
      "t3.key_clmn_nm, t3.flat_mode_cd, t3.dim_clmn_nm, t3.ext_condition " +
      "FROM h50_flat_rule_config t1 " +
      "JOIN h50_calc_mode t2 ON t1.indx_calc_mode_cd = t2.indx_calc_mode_cd " +
      "JOIN h50_indx_tbl_info t3 ON t1.indx_tbl_nm = t3.indx_tbl_nm " +
      "AND t1.inds_cls_cd = t3.inds_cls_cd " +
      "AND t1.indx_cat_cd = t3.indx_cat_cd " +
      "WHERE t1.active_ind = 1 " +
      "AND t3.inds_cls_cd = '" + inds_cls_cd + "' " +
      "AND t1.indx_cat_cd = '" + indx_cat_cd + "' "

    println("SQL for getting flat rule is:\n" + sql)

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
          rs.getString("flat_mode_cd"),
          rs.getString("dim_clmn_nm").toLowerCase,
          rs.getString("key_clmn_nm").toLowerCase,
          rs.getString("ext_condition").toLowerCase)
        itemList += item
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      JDBCUtils.closeConn(rs, ps, conn)
    }


    val flatConfigure = itemList.groupBy(
      cfg => FlatTableConfig(cfg.indx_tbl_nm, cfg.inds_cls_cd, cfg.flat_mode_cd, cfg.dim_clmn_nm, cfg.key_clmn_nm, cfg.ext_condition)
    ).mapValues(
      clmns => (for (c <- clmns) yield FlatColumnConfig(c.indx_clmn_nm, c.statt_indx_id, c.dim_id, c.indx_calc_mode_cd)).toList
    )

    if (TransformerConfigure.isDebug)
      flatConfigure.foreach(println)

    flatConfigure
  }

  def getIndexTableQueryStmt(tableConfig: FlatTableConfig, columnConfig: List[FlatColumnConfig])
  : String = {
    "SELECT " + tableConfig.key_clmn_nm + ", " +
      (for (c <- columnConfig) yield c.indx_clmn_nm).distinct.reduce(_ + ", " + _) +
      (if (tableConfig.flat_mode_cd.equals("20"))
        ", " + List("statt_dt", "statt_indx_id", tableConfig.dim_clmn_nm).reduce(_ + ", " + _)
      else
        ""
        ) + " " +
      "FROM " + tableConfig.indx_tbl_nm + " " +
      "WHERE " + getIndexTableQueryCondition(tableConfig, columnConfig)

  }

  def getIndexTableQueryCondition(tableConfig: FlatTableConfig, columnConfig: List[FlatColumnConfig])
  : String = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    tableConfig.key_clmn_nm + " is not null " +
      (
        if (tableConfig.ext_condition.length > 0) {
          "and ( " + tableConfig.ext_condition + ") "
        } else ""
        ) +
      (
        if (tableConfig.flat_mode_cd.equals("20")) {
          " and ( " +
            (for (x <- columnConfig) yield
              "statt_indx_id = '" + x.statt_indx_id + "' and " + tableConfig.dim_clmn_nm + " = '" + x.dim_id + "' " +
                (x.indx_calc_mode_cd match {
                  case "10" => "and statt_dt = '" + sdf.format(DateUtils.today) + "' "
                  case "20" => "and statt_dt > '" + sdf.format(DateUtils.getDateBeforeDays(7)) +
                    "' and statt_dt <= '" + sdf.format(DateUtils.today) + "' "
                  case "21" => "and statt_dt > '" + sdf.format(DateUtils.getDateBeforeDays(30)) +
                    "' and statt_dt <= '" + sdf.format(DateUtils.today) + "' "
                  case _ => "1=0 "
                })).reduce(_ + " OR " + _) +
            ") "
        } else
          ""
        )
  }
}

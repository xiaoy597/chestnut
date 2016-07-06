package com.hd.bigdata

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Yu on 16/5/10.
  */
object TestData {
  def getFlatRule(): List[FlatRuleConfig] = {
    List()
//    List(
//      FlatRuleConfig(
//        "h52_ftb_cust_integrate_info_g",
//        "1100",
//        "Cur_Vip_Score",
//        "X",
//        "X",
//        "10",
//        "直接取数",
//        "flat_index_tbl",
//        "Cur_Vip_Score",
//        "1"
//      ),
//      FlatRuleConfig(
//        "H52_Inds_Statt_Indx_Rslt_G",
//        "1100",
//        "Day_Indx_Val",
//        "IDX000002",
//        "PRD000002",
//        "21",
//        "汇总30天",
//        "flat_index_tbl",
//        "invalid_column_name",
//        "1"
//      ),
//      FlatRuleConfig(
//        "H52_Inds_Statt_Indx_Rslt_G",
//        "1100",
//        "Rec_90d_Indx_Val",
//        "IDX000001",
//        "PRD000002",
//        "10",
//        "直接取数",
//        "flat_index_tbl",
//        "invalid_column_name",
//        "1"
//      ),
//      FlatRuleConfig(
//        "H52_Inds_Statt_Indx_Rslt_G",
//        "1100",
//        "Rec_60d_Indx_Val",
//        "IDX000001",
//        "PRD000002",
//        "10",
//        "直接取数",
//        "flat_index_tbl",
//        "invalid_column_name",
//        "1"
//      ),
//      FlatRuleConfig(
//        "H52_Inds_Statt_Indx_Rslt_G",
//        "1100",
//        "Day_Indx_Val",
//        "IDX000001",
//        "PRD000001",
//        "10",
//        "直接取数",
//        "flat_index_tbl",
//        "invalid_column_name",
//        "1"
//      ),
//      FlatRuleConfig(
//        "H52_Inds_Statt_Indx_Rslt_G",
//        "1100",
//        "Day_Indx_Val",
//        "IDX000001",
//        "PRD000001",
//        "20",
//        "汇总7天",
//        "flat_index_tbl",
//        "invalid_column_name",
//        "1"
//      )
//    )
  }

  def getNumericIndexData(sc: SparkContext): RDD[CustomerProdGrpIndexData] = {
    sc.parallelize(
      List(
        CustomerProdGrpIndexData(
          "2016-04-20",
          "00",
          2,
          "PRD000002",
          "IDX000002",
          100,
          200,
          300,
          400,
          500,
          "2016-05-10",
          100000,
          200000,
          300000,
          400000,
          "00"
        ),
        CustomerProdGrpIndexData(
          "2016-05-10",
          "00",
          2,
          "PRD000002",
          "IDX000001",
          100,
          200,
          300,
          400,
          500,
          "2016-05-10",
          100000,
          200000,
          300000,
          400000,
          "00"
        ),
        CustomerProdGrpIndexData(
          "2016-05-10",
          "00",
          1,
          "PRD000001",
          "IDX000001",
          100,
          200,
          300,
          400,
          500,
          "2016-05-10",
          100000,
          200000,
          300000,
          400000,
          "00"
        ),
        CustomerProdGrpIndexData(
          "2016-05-09",
          "00",
          1,
          "PRD000001",
          "IDX000001",
          101,
          200,
          300,
          400,
          500,
          "2016-05-10",
          100000,
          200000,
          300000,
          400000,
          "00"
        ),
        CustomerProdGrpIndexData(
          "2016-05-09",
          "00",
          1,
          "PRD000003",
          "IDX000001",
          101,
          200,
          300,
          400,
          500,
          "2016-05-10",
          100000,
          200000,
          300000,
          400000,
          "00"
        ),
        CustomerProdGrpIndexData(
          "2016-04-09",
          "00",
          1,
          "PRD000001",
          "IDX000001",
          101,
          200,
          300,
          400,
          500,
          "2016-05-10",
          100000,
          200000,
          300000,
          400000,
          "00"
        )

      )
    )
  }

  def getSoccerIndexData(sc: SparkContext): RDD[SoccerIndexData] = {
    sc.parallelize(List(
      SoccerIndexData(
        1 //统一客户编号
        , "2010-02-02" //首次购买单场票时间
        , "2015-05-09" //最近一次购买单场票时间
        , "2013-04-23" //首次购买套票时间
        , "2014-02-20" //最近一次购买套票时间
        , "2010-02-03" //首次购买球衣时间
        , "2016-01-30" //最近一次购买球衣时间
        , "2012-10-11" //首次购买纪念品时间
        , "2015-04-20" //最近一次购买纪念品时间
        , 1 //是否单场注册用户
        , 1 //是否套票注册用户
        , 1 //是否购买过单场票
        , 1 //是否购买过套票
        , 1 //是否购买过球衣
        , 1 //是否购买过纪念品
        , 15400 //当前积分
        , 2100 //已用积分
        , 17500 //累计积分
        , "XX省XX市XX区XX路XX号" //常用收货地址
        , "2016-05-10" //数据日期
      )
    ))
  }
}

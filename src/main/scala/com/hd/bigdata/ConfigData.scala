package com.hd.bigdata

import java.util.Date

/**
  * Created by Yu on 16/5/9.
  */
case class TransformRule1(
                          indx_tbl_nm: String, // '指标表名称',
                          indx_clmn_nm: String, // '指标表字段名称',
                          statt_indx_id: String, // '统计指标编号',
                          dim_id: String, // '维度ID',
                          indx_calc_mode_cd: String, // '指标计算方式代码',
                          flat_tbl_nm: String, //'扁平表名称 自动生成，根据扁平化方式不同 直接：指标表表名+指标表字段名 行转列：维度ID+指标ID+度量字段名',
                          flat_clmn_nm: String, //'扁平表字段名称',
                          active_ind: String, // '有效标志 1：有效；0：无效',
                          created_ts: Date, // '创建时间',
                          updated_ts: Date // '最近更新时间',
                        )
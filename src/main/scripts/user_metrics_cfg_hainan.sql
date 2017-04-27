# noinspection SqlNoDataSourceInspectionForFile
USE user_profile;

DROP TABLE IF EXISTS h50_indx_tbl_info;
CREATE TABLE h50_indx_tbl_info
(
  indx_cat_cd char(1) NOT NULL COMMENT '指标体系代码',
  indx_tbl_nm varchar(60) NOT NULL COMMENT '指标表名称',
  indx_tbl_desc varchar(100) COMMENT '指标表描述',
  inds_cls_cd varchar(4) COMMENT '产业分类代码',
  flat_mode_cd  varchar(2) COMMENT '扁平化方式代码',
  dim_clmn_nm varchar(60) COMMENT '维度字段名称',
  key_clmn_nm varchar(60) COMMENT '指标键字段名称',
  ext_condition varchar(500) COMMENT '指标表查询条件',
  created_ts  datetime COMMENT '创建时间',
  updated_ts  datetime COMMENT '最近更新时间',
  PRIMARY KEY (`indx_cat_cd`, `indx_tbl_nm`, `inds_cls_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '指标表信息表';

insert into h50_indx_tbl_info values ('0', 'H50_Popu_Base_Info_Csum_Metrics', '流动人口基础信息汇总-标签表', '1000', '10', '', 'unif_popu_id', '', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

# 指标体系信息表
DROP TABLE IF EXISTS h50_indx_cat_info;
CREATE TABLE h50_indx_cat_info
(
  indx_cat_cd char(1) NOT NULL COMMENT '指标体系代码',
  indx_cat_nm varchar(30) NOT NULL COMMENT '指标体系名称',
  metrics_tbl_nm varchar(60) COMMENT '指标宽表名称',
  tag_tbl_nm varchar(60) COMMENT '标签宽表名称',
  PRIMARY KEY (`indx_cat_cd`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '指标体系信息表';

insert into h50_indx_cat_info values ('0', '人口指标体系', 'user_metrics_test', 'user_discrete_metrics_test');


#不需要配置
DROP TABLE IF EXISTS h50_indx_clmn_info;
CREATE TABLE h50_indx_clmn_info
(
  indx_cat_cd char(1) NOT NULL COMMENT '指标体系代码',
  indx_tbl_nm varchar(60) NOT NULL COMMENT '指标表名称',
  indx_clmn_nm varchar(60) COMMENT '指标表字段名称',
  indx_clmn_desc varchar(100) COMMENT '指标表字段描述',
  msmt_clmn_ind  char(1) COMMENT '度量字段标识  1:度量 0:非度量',
  created_ts  datetime COMMENT '创建时间',
  updated_ts  datetime COMMENT '最近更新时间',
  PRIMARY KEY (`indx_cat_cd`, `indx_tbl_nm`,`indx_clmn_nm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '指标表字段信息表';

#不需要配置
DROP TABLE IF EXISTS h50_flat_mode;
CREATE TABLE h50_flat_mode
(
  flat_mode_cd  varchar(2) NOT NULL COMMENT '扁平化方式代码',
  flat_mode_nm  varchar(20) COMMENT '扁平化方式名称 10：直接扁平化 20：行转列',
  created_ts  datetime COMMENT '创建时间',
  updated_ts  datetime COMMENT '最近更新时间',
  PRIMARY KEY (`flat_mode_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '扁平化方式代码表';

insert into h50_flat_mode values ('10', '直接扁平化', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_flat_mode values ('20', '行转列', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

#不需要配置
DROP TABLE IF EXISTS h50_calc_mode;
CREATE TABLE h50_calc_mode
(
  indx_calc_mode_cd  varchar(2) NOT NULL COMMENT '指标计算方式代码',
  indx_calc_mode_nm  varchar(20) COMMENT '指标计算方式名称 10：直接取数 20：汇总-最近7天 21：汇总-最近30天 99：未加工',
  created_ts  datetime COMMENT '创建时间',
  updated_ts  datetime COMMENT '最近更新时间',
  PRIMARY KEY (`indx_calc_mode_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '指标计算方式表';

insert into h50_calc_mode values ('10' ,'直接取数', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_calc_mode values ('20' ,'汇总-最近7天', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_calc_mode values ('21' ,'汇总-最近30天', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_calc_mode values ('22' ,'汇总-最近10天', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_calc_mode values ('23' ,'汇总-最近15天', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_calc_mode values ('99' ,'未加工', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

DROP TABLE IF EXISTS h50_flat_rule_config;
CREATE TABLE h50_flat_rule_config
(
  indx_cat_cd char(1) NOT NULL COMMENT '指标体系代码',
  indx_tbl_nm varchar(60) NOT NULL COMMENT '指标表名称',
  inds_cls_cd varchar(4) COMMENT '产业分类代码',
  indx_clmn_nm  varchar(60) NOT NULL COMMENT '指标表字段名称',
  statt_indx_id varchar(20) NOT NULL COMMENT '统计指标编号',
  dim_id  varchar(60) NOT NULL COMMENT '维度ID',
  indx_calc_mode_cd char(2) NOT NULL COMMENT '指标计算方式代码',
  flat_tbl_nm varchar(60) COMMENT '扁平表名称 自动生成，根据扁平化方式不同 直接：指标表表名+指标表字段名 行转列：维度ID+指标ID+度量字段名',
  flat_clmn_nm  varchar(60) COMMENT '扁平表字段名称',
  active_ind  char(2) COMMENT '有效标志 1：有效；0：无效',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`indx_cat_cd`, `indx_tbl_nm`, `inds_cls_cd`, `indx_clmn_nm`,`statt_indx_id`,`dim_id`,`indx_calc_mode_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '扁平化规则配置表';

#insert into h50_flat_rule_config values ('0', 'h52_inds_statt_indx_rslt_g', '1100', 'Day_Indx_Val', 'MSMIDX0102', 'MSMGRP010101', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '1100', 'Day_Indx_Val', 'MSMIDX0102', 'MSMGRP010104', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '1100', 'Day_Indx_Val', 'MSMIDX0705', 'MSMGRP010101', '20', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '1100', 'Day_Indx_Val', 'MSMIDX0705', 'MSMGRP010104', '21', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

#insert into h50_flat_rule_config values ('0', 'h52_inds_statt_indx_rslt_g', '3110', 'Day_Indx_Val', 'FTBIDX0101', 'FTS040211', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '3110', 'Day_Indx_Val', 'FTBIDX0201', 'FTS040213', '21', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_ftb_cust_integrate_info_g', '3110', 'Fst_Buy_Sig_Tkt_Dt', '', '', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('0', 'h52_ftb_cust_integrate_info_g', '3110', 'Cur_Vip_Score', '', '', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_ftb_cust_integrate_info_g', '3110', 'vip_reg_user_ind', '', '', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

#insert into h50_flat_rule_config values ('0', 'h52_inds_statt_indx_rslt_g', '2000', 'Day_Indx_Val', 'HTLIDX0311', 'HTLCRSTJDLHDDXKLCF', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '2000', 'Day_Indx_Val', 'HTLIDX0101', 'HTLCRSTJDLHDDXKLGN', '21', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '2000', 'Day_Indx_Val', 'HTLIDX0109', 'HTLCRSTJDLHDDXKMTG', '20', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('0', 'h52_hotel_unif_cust_csum', '2000', 'avg_live_days', '', '', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

insert into h50_flat_rule_config values ('0', 'H50_Popu_Base_Info_Csum_Metrics', '1000', 'Age', '', '', '10', 'user_metrics_test', '年龄段代码', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_flat_rule_config values ('0', 'H50_Popu_Base_Info_Csum_Metrics', '1000', 'Gender_Cd', '', '', '10', 'user_metrics_test', '性别代码', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('0', 'H50_Popu_Base_Info_Csum_Metrics', '1000', 'Avg_Stay_Tm', '', '', '10', 'user_metrics_test', '来琼平均停留时间', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('0', 'H50_Popu_Base_Info_Csum_Metrics', '1000', 'Birth_Years_Cd', '', '', '10', 'user_metrics_test', '出生年代', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('0', 'H50_Popu_Base_Info_Csum_Metrics', '1000', 'Coop_Ind', '', '', '10', 'user_metrics_test', '农村社保标识', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('0', 'H50_Popu_Base_Info_Csum_Metrics', '1000', 'Cur_Insure_Stat_Cd', '', '', '10', 'user_metrics_test', '当前参保状态代码', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('0', 'H50_Popu_Base_Info_Csum_Metrics', '1000', 'Domn_Cd', '', '', '10', 'user_metrics_test', '产业属性', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('0', 'H50_Popu_Base_Info_Csum_Metrics', '1000', 'Edu_Degr_Cd', '', '', '10', 'user_metrics_test', '文化程度代码', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('0', 'H50_Popu_Base_Info_Csum_Metrics', '1000', 'Family_Popu_Cnt', '', '', '10', 'user_metrics_test', '低保家庭人口数', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');


-- ######################### 离散化 ####################################

DROP TABLE IF EXISTS h50_dsps_tag_mapping;
CREATE TABLE h50_dsps_tag_mapping
(
  flat_tbl_nm varchar(60) NOT NULL COMMENT '扁平表名称 自动生成，根据扁平化方式不同 直接：指标表表名+指标表字段名 行转列：维度ID+指标ID+度量字段名',
  flat_clmn_nm  varchar(100) NOT NULL COMMENT '扁平表字段名称',
  tag_ctgy_id  int COMMENT '标签ID',
  dsps_alg_id  varchar(30)  COMMENT '离散化算法ID',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`flat_tbl_nm`,`flat_clmn_nm`,`tag_ctgy_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '离散化标签映射表';

insert into h50_dsps_tag_mapping values ('user_metrics_test', 'H50_Popu_Base_Info_Csum_Metrics.Age', 10010101, 'DSPS_ALG_0001', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_tag_mapping values ('user_metrics_test', 'H50_Popu_Base_Info_Csum_Metrics.Gender_Cd', 10010102, 'DSPS_ALG_0003', '2016-05-23 08:00:00', '2016-05-23 08:00:00');


DROP TABLE IF EXISTS h50_dsps_alg_info;
CREATE TABLE h50_dsps_alg_info
(
  dsps_alg_id varchar(30) NOT NULL COMMENT '离散化算法ID',
  dsps_alg_desc  varchar(100) COMMENT '离散化算法描述',
  dsps_alg_type_cd  char(2) COMMENT '离散化算法类型代码',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`dsps_alg_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '离散化算法信息表';

insert into h50_dsps_alg_info values ('DSPS_ALG_0001', '年龄分段', '10', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_info values ('DSPS_ALG_0003', '性别枚举', '30', '2016-05-23 08:00:00', '2016-05-23 08:00:00');


DROP TABLE IF EXISTS h50_dsps_alg_rule_para;
CREATE TABLE h50_dsps_alg_rule_para
(
  dsps_alg_id varchar(30) NOT NULL COMMENT '离散化算法ID',
  rule_para_seq  int NOT NULL COMMENT '规则参数序号',
  rule_para_value  varchar(100) COMMENT '规则参数值',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`dsps_alg_id`, `rule_para_seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '离散化算法规则参数表';

insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 1, '0', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 2, '10', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 3, '10', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 4, '20', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 5, '20', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 6, '30', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 7, '30', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 8, '40', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 9, '40', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 10, '50', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 11, '50', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 12, '60', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 13, '60', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 14, '70', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 15, '70', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 16, '80', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 17, '80', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 18, '90', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 19, '90', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 20, '100', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 21, '100', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 22, '200', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0003', 1, '01', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
# insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0002', 1, '0', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
# insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0002', 2, '100000', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
# insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0002', 3, '1000', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
# insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0003', 1, '02', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

DROP TABLE IF EXISTS h50_dsps_alg_type;
CREATE TABLE h50_dsps_alg_type
(
  dsps_alg_type_cd  char(2) NOT NULL COMMENT '离散化算法类型代码',
  dsps_alg_type_nm  varchar(20) COMMENT '离散化算法类型名称',
  dsps_alg_type_desc  varchar(100) COMMENT '离散化算法类型描述',
  -- 10：指定-根据需求人为指定上下限进行分段离散
  -- 20：模板-基于预先定义的分段方式进行分段离散
  -- 30：枚举-通过穷举方式对指标进行分组离散
  -- 40：等距-按照固定的步长进行分段离散
  -- 41：等量-按照数量特点进行分段离散
  -- 42：特征-按照指标的变化特征进行分段离散',
  dsps_alg_rule_cd  char(2) COMMENT '离散化算法规则代码',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`dsps_alg_type_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '离散化算法类型代码表';

DROP TABLE IF EXISTS h50_dsps_alg_rule;
CREATE TABLE h50_dsps_alg_rule
(
  dsps_alg_rule_cd  char(2) NOT NULL COMMENT '离散化算法规则代码',
  dsps_alg_rule_desc  varchar(100) COMMENT '离散化算法规则描述 ',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`dsps_alg_rule_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '离散化算法规则代码表';

DROP TABLE IF EXISTS h50_dsps_mapping_para;
CREATE TABLE h50_dsps_mapping_para
(
  dsps_mapping_para_class     varchar(8) NOT NULL COMMENT '映射参数类别',
  dsps_mapping_para_key       varchar(100) NOT NULL COMMENT '映射键',
  dsps_mapping_para_value     varchar(100) NOT NULL COMMENT '映射值',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`dsps_mapping_para_class`, `dsps_mapping_para_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '离散映射参数表';

insert into h50_dsps_mapping_para values ('01', '10', '01', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('01', '20', '02', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('01', '99', '03', '2016-05-23 08:00:00', '2016-05-23 08:00:00');



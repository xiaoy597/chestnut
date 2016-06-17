# noinspection SqlNoDataSourceInspectionForFile
USE user_profile;

DROP TABLE h50_indx_tbl_info;
CREATE TABLE h50_indx_tbl_info
(
  indx_tbl_nm varchar(30) NOT NULL COMMENT '指标表名称',
  indx_tbl_desc varchar(100) COMMENT '指标表描述',
  inds_cls_cd varchar(4) COMMENT '产业分类代码',
  flat_mode_cd  varchar(2) COMMENT '扁平化方式代码',
  dim_clmn_nm varchar(20) COMMENT '维度字段名称',
  created_ts  datetime COMMENT '创建时间',
  updated_ts  datetime COMMENT '最近更新时间',
  PRIMARY KEY (`indx_tbl_nm`, `inds_cls_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '指标表信息表';

insert into h50_indx_tbl_info values ('h52_inds_statt_indx_rslt_g', '', '1100', '20', 'prod_grp_id', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_indx_tbl_info values ('h52_inds_statt_indx_rslt_g', '', '3110', '20', 'prod_grp_id', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_indx_tbl_info values ('h52_ftb_cust_integrate_info_g', '', '3110', '10', '', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_indx_tbl_info values ('h52_inds_statt_indx_rslt_g', '', '2000', '20', 'prod_grp_id', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_indx_tbl_info values ('h52_hotel_unif_cust_csum', '', '2000', '10', '', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_indx_tbl_info values ('h52_cust_inds_merge','','','10','','2016-06-16 08:00:00','2016-06-16 08:00:00');
insert into h50_indx_tbl_info values ('h52_estt_cust_integrate_info','','1100','10','','2016-06-16 08:00:00','2016-06-16 08:00:00');

#主键还得包括列？
DROP TABLE h50_indx_clmn_info;
CREATE TABLE h50_indx_clmn_info
(
  indx_tbl_nm varchar(30) NOT NULL COMMENT '指标表名称',
  indx_clmn_nm varchar(30) COMMENT '指标表字段名称',
  indx_clmn_desc varchar(100) COMMENT '指标表字段描述',
  msmt_clmn_ind  char(1) COMMENT '度量字段标识  1:度量 0:非度量',
  created_ts  datetime COMMENT '创建时间',
  updated_ts  datetime COMMENT '最近更新时间',
  PRIMARY KEY (`indx_tbl_nm`,`indx_clmn_nm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '指标表字段信息表';

insert into h50_indx_clmn_info values ('h52_inds_statt_indx_rslt_g', 'Day_Indx_Val', '当日指标值', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_indx_clmn_info values ('h52_inds_statt_indx_rslt_g', 'Month_Indx_Val', '当月指标值', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_indx_clmn_info values ('h52_inds_statt_indx_rslt_g', 'Quarter_Indx_val', '当季指标值', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_indx_clmn_info values ('h52_inds_statt_indx_rslt_g','Accm_Indx_Val','累计指标','1','2016-06-16 08:00:00','2016-06-16 08:00:00');

DROP TABLE h50_flat_mode;
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

DROP TABLE h50_calc_mode;
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
insert into h50_calc_mode values ('99' ,'未加工', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

DROP TABLE h50_flat_rule_config;
CREATE TABLE h50_flat_rule_config
(
  indx_tbl_nm varchar(30) NOT NULL COMMENT '指标表名称',
  inds_cls_cd varchar(4) COMMENT '产业分类代码',
  indx_clmn_nm  varchar(30) NOT NULL COMMENT '指标表字段名称',
  statt_indx_id varchar(20) NOT NULL COMMENT '统计指标编号',
  dim_id  varchar(40) NOT NULL COMMENT '维度ID',
  indx_calc_mode_cd char(2) NOT NULL COMMENT '指标计算方式代码',
  flat_tbl_nm varchar(30) COMMENT '扁平表名称 自动生成，根据扁平化方式不同 直接：指标表表名+指标表字段名 行转列：维度ID+指标ID+度量字段名',
  flat_clmn_nm  varchar(30) COMMENT '扁平表字段名称',
  active_ind  char(2) COMMENT '有效标志 1：有效；0：无效',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`indx_tbl_nm`, `inds_cls_cd`, `indx_clmn_nm`,`statt_indx_id`,`dim_id`,`indx_calc_mode_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '扁平化规则配置表';

insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '1100', 'Day_Indx_Val', 'MSMIDX0102', 'MSMGRP010101', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '1100', 'Day_Indx_Val', 'MSMIDX0102', 'MSMGRP010104', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '1100', 'Day_Indx_Val', 'MSMIDX0705', 'MSMGRP010101', '20', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '1100', 'Day_Indx_Val', 'MSMIDX0705', 'MSMGRP010104', '21', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '3110', 'Day_Indx_Val', 'FTBIDX0101', 'FTS040211', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '3110', 'Day_Indx_Val', 'FTBIDX0201', 'FTS040213', '21', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_ftb_cust_integrate_info_g', '3110', 'Fst_Buy_Sig_Tkt_Dt', '', '', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_flat_rule_config values ('h52_ftb_cust_integrate_info_g', '3110', 'Cur_Vip_Score', '', '', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_ftb_cust_integrate_info_g', '3110', 'vip_reg_user_ind', '', '', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '2000', 'Day_Indx_Val', 'HTLIDX0311', 'HTLCRSTJDLHDDXKLCF', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '2000', 'Day_Indx_Val', 'HTLIDX0101', 'HTLCRSTJDLHDDXKLGN', '21', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_flat_rule_config values ('h52_inds_statt_indx_rslt_g', '2000', 'Day_Indx_Val', 'HTLIDX0109', 'HTLCRSTJDLHDDXKMTG', '20', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_flat_rule_config values ('h52_hotel_unif_cust_csum', '2000', 'avg_live_days', '', '', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

insert into h50_flat_rule_config values ('h52_cust_inds_merge', '', 'idtfy_info', '', '', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_flat_rule_config values ('h52_estt_cust_integrate_info', '1100', 'hous_nums', '', '', '10', 'user_metrics_test', '', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');


-- ######################### 离散化 ####################################

DROP TABLE h50_dsps_tag_mapping;
CREATE TABLE h50_dsps_tag_mapping
(
  flat_tbl_nm varchar(30) NOT NULL COMMENT '扁平表名称 自动生成，根据扁平化方式不同 直接：指标表表名+指标表字段名 行转列：维度ID+指标ID+度量字段名',
  flat_clmn_nm  varchar(100) NOT NULL COMMENT '扁平表字段名称',
  tag_id  int COMMENT '标签ID',
  dsps_alg_id  varchar(30)  COMMENT '离散化算法ID',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`flat_tbl_nm`,`flat_clmn_nm`,`tag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '离散化标签映射表';

insert into h50_dsps_tag_mapping values ('user_metrics_test', 'metric.MSMGRP010101.MSMIDX0102.day_indx_val.current', 1, 'DSPS_ALG_0001', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
#insert into h50_dsps_tag_mapping values ('user_metrics_test', 'idx_MSMGRP010104MSMIDX0705Day_Indx_Val21', 2, 'DSPS_ALG_0002', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_tag_mapping values ('user_metrics_test', 'h52_estt_cust_integrate_info.hous_nums', 3, 'DSPS_ALG_0003', '2016-05-23 08:00:00', '2016-05-23 08:00:00');


DROP TABLE h50_dsps_alg_info;
CREATE TABLE h50_dsps_alg_info
(
  dsps_alg_id varchar(30) NOT NULL COMMENT '离散化算法ID',
  dsps_alg_desc  varchar(100) COMMENT '离散化算法描述',
  dsps_alg_type_cd  char(2) COMMENT '离散化算法类型代码',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`dsps_alg_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '离散化算法信息表';

insert into h50_dsps_alg_info values ('DSPS_ALG_0001', '', '10', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_info values ('DSPS_ALG_0002', '', '40', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_info values ('DSPS_ALG_0003', '', '30', '2016-05-23 08:00:00', '2016-05-23 08:00:00');


DROP TABLE h50_dsps_alg_rule_para;
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
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 2, '1000', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 3, '1000', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 4, '5000', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 5, '5000', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0001', 6, '10000', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0002', 1, '0', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0002', 2, '100000', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0002', 3, '1000', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_alg_rule_para values ('DSPS_ALG_0003', 1, '02', '2016-05-23 08:00:00', '2016-05-23 08:00:00');

DROP TABLE h50_dsps_alg_type;
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

DROP TABLE h50_dsps_alg_rule;
CREATE TABLE h50_dsps_alg_rule
(
  dsps_alg_rule_cd  char(2) NOT NULL COMMENT '离散化算法规则代码',
  dsps_alg_rule_desc  varchar(100) COMMENT '离散化算法规则描述 ',
  -- 10：#上限值#下限值#，左开右闭
  -- 20：#参数1#参数2#参数3# 等量：#上限值#下限值#，左开右闭
  -- 30：#起始值#终止值#步长#，左开右闭',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`dsps_alg_rule_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '离散化算法规则代码表';

DROP TABLE h50_dsps_mapping_para;
CREATE TABLE h50_dsps_mapping_para
(
  dsps_mapping_para_class     char(2) NOT NULL COMMENT '映射参数类别',
  dsps_mapping_para_key       varchar(100) NOT NULL COMMENT '映射键',
  dsps_mapping_para_value     varchar(100) NOT NULL COMMENT '映射值',
  created_ts  datetime  COMMENT '创建时间',
  updated_ts  datetime  COMMENT '最近更新时间',
  PRIMARY KEY (`dsps_mapping_para_class`, `dsps_mapping_para_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '离散映射参数表';

insert into h50_dsps_mapping_para values ('01', '00', '0', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('01', '01', '0', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('01', '02', '0', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('01', '10', '1', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('01', '11', '2', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('02', '0', 'F', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('02', '1', 'T', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('02', '2', 'T', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('02', '3', 'T', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('02', '4', 'T', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('02', '5', 'T', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('02', '6', 'T', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('02', '7', 'T', '2016-05-23 08:00:00', '2016-05-23 08:00:00');
insert into h50_dsps_mapping_para values ('02', '8', 'T', '2016-05-23 08:00:00', '2016-05-23 08:00:00');



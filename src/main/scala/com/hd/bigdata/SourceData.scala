package com.hd.bigdata

/**
  * Created by Yu on 16/5/9.
  */
case class CustomerProdGrpIndexData(
                                     Statt_Dt: String,
                                     Statt_Dt_Typ_Cd: String,
                                     Gu_Indv_Id: String,
                                     Prod_Grp_ID: String,
                                     Statt_Indx_ID: String,
                                     Day_Indx_Val: BigDecimal,
                                     Month_Indx_Val: BigDecimal,
                                     Quarter_Indx_val: BigDecimal,
                                     Year_Indx_Val: BigDecimal,
                                     Accm_Indx_Val: BigDecimal,
                                     Data_Dt: String,
                                     Rec_60d_Indx_Val: BigDecimal,
                                     Rec_90d_Indx_Val: BigDecimal,
                                     Rec_180d_Indx_Val: BigDecimal,
                                     Rec_360d_Indx_Val: BigDecimal,
                                     Inds_Cls_Cd: String
                                   )

case class SoccerIndexData(
                            Gu_Indv_Id: String //统一客户编号
                            , Fst_Buy_Sig_Tkt_Dt: String //首次购买单场票时间
                            , Lat_Buy_Sig_Tkt_Dt: String //最近一次购买单场票时间
                            , Fst_Buy_Vip_Tkt_Dt: String //首次购买套票时间
                            , Lat_Buy_Vip_Tkt_Dt: String //最近一次购买套票时间
                            , Fst_Buy_Shirt_Dt: String //首次购买球衣时间
                            , Lat_Buy_Shirt_Dt: String //最近一次购买球衣时间
                            , Fst_Buy_Souvenirs_Dt: String //首次购买纪念品时间
                            , Lat_Buy_Souvenirs_Dt: String //最近一次购买纪念品时间
                            , sig_reg_user_ind: Int //是否单场注册用户
                            , vip_reg_user_ind: Int //是否套票注册用户
                            , buy_sig_tkt_ind: Int //是否购买过单场票
                            , buy_vip_tkt_ind: Int //是否购买过套票
                            , buy_shirt_ind: Int //是否购买过球衣
                            , buy_souvenirs_ind: Int //是否购买过纪念品
                            , Cur_Vip_Score: BigDecimal //当前积分
                            , Used_Vip_Score: BigDecimal //已用积分
                            , Accm_Vip_Score: BigDecimal //累计积分
                            , Com_Recb_Addr: String //常用收货地址
                            , Data_Dt: String //数据日期
                          )

class HotelIndexData(
                      val gu_indv_id: String,
                      val hotel_mem_ind: Int,
                      val lodger_ind: Int,
                      val bevrg_cust_ind: Int,
                      val rest_mem_ind: Int,
                      val recrn_cust_ind: Int,
                      val sport_mem_ind: Int,
                      val memb_crd_qty: Int,
                      val point_bal: Int,
                      val accm_point: Int,
                      val mem_crd_bal: BigDecimal,
                      val mem_crd_accm_rec_amt: BigDecimal,
                      val mem_crd_accm_consm_amt: BigDecimal,
                      val hotel_urbn_qty: Int,
                      val room_bookn_ttl_cnt: Int,
                      val live_ttl_cnt: Int,
                      val bookn_live_ttl_days: Int,
                      val live_ttl_days: Int,
                      val avg_live_days: Int,
                      val max_live_days: Int,
                      val max_live_in_cnt: Int,
                      val live_consm_amt: BigDecimal,
                      val rest_consm_amt: BigDecimal,
                      val met_consm_amt: BigDecimal,
                      val spa_consm_amt: BigDecimal,
                      val sport_consm_amt: BigDecimal,
                      val ent_consm_amt: BigDecimal,
                      val oth_consm_amt: BigDecimal,
                      val consm_totl_amt: BigDecimal,
                      val hyear_total_amt: BigDecimal,
                      val mon_total_amt: BigDecimal,
                      val wek_total_amt: BigDecimal,
                      val avg_consm_amt: BigDecimal,
                      val with_child_ind: Int,
                      val ear_live_dt: String,
                      val rec_live_dt: String,
                      val rec_hyear_live_cnt: Int,
                      val rec_mon_live_cnt: Int,
                      val rec_wek_live_cnt: Int,
                      val room_typ_cd: String,
                      val com_bookn_chnl: String,
                      val com_pay_typ: String,
                      val com_bookn_cust_typ_cd: String,
                      val now_diff_ear: Int,
                      val now_diff_rec: Int,
                      val rec_diff_ear: Int,
                      val ear_week_day: String,
                      val ear_mon: String,
                      val data_dt: String
                    ) extends Serializable

class EstateIndexData(
                       val gu_indv_id: String, // 个人客户统一编号
                       val gender_cd: String, // 性别
                       val age: Int, // 年龄
                       val buy_hous_ind: Int, // 购房用户标志
                       val hous_nums: Int, // 客户总购房套数
                       val max_bld_area: BigDecimal, // 建筑最大面积
                       val hous_value: BigDecimal, // 房产总价值
                       val estt_loan_typ: String, // 贷款类型
                       val aj_loan_totl_amt: BigDecimal, // 按揭贷款总额
                       val gjj_loan_totl_amt: BigDecimal, // 公积金贷款总额
                       val aj_loan_max_year: Int, // 按揭贷款最长年限
                       val gjj_loan_max_year: Int, // 公积金贷款最长年限
                       val gjj_loan_pct: BigDecimal, // 公积金贷款金额占比
                       val min_contr_dt: String, // 最早合同日期
                       val min_hdin_dt: String, // 最早交房日期
                       val sell_prog_num: Int, // 销售过程次数
                       val bef_contr_touch_ind: Int, // 签合同前是否有接触
                       val contr_cmfm_opport_ind: Int, // 合同是否来源于机会
                       val salestage: Int, // 最终购房阶段
                       val istgtherbuyhou: Int, // 是否两人共买一套
                       val bproducttypecode: String, // 房产类型
                       val roomstru: String, // 房型
                       val data_dt: String // 数据日期
                     ) extends Serializable
{
  override def toString(): String = {
    "gu_indv_id:" + gu_indv_id +
      ",gender_cd:" + gender_cd +
      ",age:" + age +
      ",buy_hous_ind:" + buy_hous_ind +
      ",hous_nums:" + hous_nums +
      ",..."
  }
}

class CommonCustomerIndexData(
                               val gu_indv_id: String, // 个人客户统一编号
                               val cust_nm: String, // 客户姓名
                               val idtfy_info: String, // 身份证号码
                               val birth_dt: String, // 出生日期
                               val cust_age: Int, // 客户年龄
                               val gender_cd: String, // 性别代码
                               val admin_regn_id: String, // 行政区域编号
                               val prov_id: String, // 归属省份编号
                               val cty_id: String, // 归属城市编号
                               val mobl_num1: String, // 手机号码1
                               val mobl_num2: String, // 手机号码2
                               val mobl_num3: String, // 手机号码3
                               val family_tel: String, // 家庭电话
                               val offi_tel: String, // 办公电话
                               val email: String, // 行业代码
                               val marrg_stat_cd: String, // 邮箱
                               val edu_degree_cd: String, // 婚姻状态代码
                               val industry_cd: String, // 学历代码
                               val job_cd: String, // 职业代码
                               val family_addr: String, // 家庭地址
                               val office_addr: String, // 公司地址
                               val estate_purc_inte_ind: Int, // 购房意向标志
                               val estate_fst_purc_inte_dt: String, // 最近购房意向日期
                               val estate_purc_ind: Int, // 购房客户标志
                               val estate_purc_fst_dt: String, // 首次购房日期
                               val estate_owner_fst_dt: String, // 首次成为业主日期
                               val lodger_ind: Int, // 客房客户标志
                               val hotel_mem_ind: Int, // 酒店会员标志
                               val hotel_fst_live_dt: String, // 酒店首次入住日期
                               val bevrg_cust_ind: Int, // 餐饮客户标志
                               val rest_mem_ind: Int, // 餐饮会员标志
                               val fst_bevrg_cust_dt: String, // 首次成为餐饮客户日期
                               val recrn_cust_ind: Int, // 康乐会员标志
                               val fst_recrn_user_dt: String, // 首次成为康乐客户日期
                               val sport_mem_ind: Int, // 运动会员标志
                               val fst_mvmt_user_ind: String, // 首次成为运动客户日期
                               val spring_cust_ind: Int, // 冰泉客户标志
                               val fst_o2o_cust_dt: String, // 首次成为农牧O2O客户日期
                               val icing_qr_code_user_ind: Int, // 冰泉扫码用户标志
                               val fst_icing_qr_code_dt: String, // 首次扫码日期
                               val spring_scan_ind: Int, // 扫码注册会员标志
                               val pmal_mem_ind: Int, // 积分商城会员标志
                               val sport_user_ind: Int, // 体育客户标志
                               val fst_sport_user_dt: String, // 首次成为体育客户日期
                               val foot_comn_usr_ind: Int, // 足球普通用户标志
                               val foot_comn_usr_fst_dt: String, // 足球普通用户注册日期
                               val annu_tick_usr_ind: Int, // 足球套票用户标志
                               val annu_tick_usr_fst_dt: String, // 足球套票用户注册日期
                               val evgrd_wld_cust_ind: Int, // 恒大世界注册用户标志
                               val hd_fax_usr_ind: Int, // 恒大金服注册用户标志
                               val hd_fax_usr_fst_dt: String, // 金服注册日期
                               val data_dt: String // 数据日期
                             ) extends Serializable {
  override def toString() : String = {
    "gu_indv_id:" + gu_indv_id +
      ",cust_nm:" + cust_nm +
      ",idtfy_info:" + idtfy_info +
      ",birth_dt:" + birth_dt +
      ",cust_age:" + cust_age +
      ",gender_cd:" + gender_cd +
      ",..."
  }
}


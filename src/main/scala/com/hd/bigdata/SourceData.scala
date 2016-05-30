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

class HotelIndexData (
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

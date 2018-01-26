alter table cid_derived_hs_1m add index index_event_cid (event_cid);
alter table cid_derived_hs_3m add index index_event_cid (event_cid);
alter table cid_derived_1m_am_all add index index_event_cid (event_cid);
alter table cid_derived_3m_am_all add index index_event_cid (event_cid);
alter table cid_derived_abn_all add index index_event_cid (event_cid);
alter table cid_first_loan_source_features add index index_event_cid (event_cid);
alter table cid_new_feature_derived_all add index index_event_cid (event_cid);

#生成最终的特征表
drop table if exists xiaohua_feature_extract_all_v3_3;
set @row_number = 0;
create table xiaohua_feature_extract_all_v3_3 as
select (@row_number:=@row_number + 1) as cid, t.*
from 
(select 
       rand()*100000 as rand,
       t0.cid as event_cid,
       t0.label,

       t1.cid_apply_1m_hf,
       t1.cid_bind_bankcardno_1m_hf,
       t1.mobile_apply_1m_hf,
       t1.mobile_bind_bankcardno_1m_hf,
       t1.certno_bind_cid_1m_hf,
       t1.bankcardno_bind_cid_1m_hf,

       t2.cid_apply_3m_hf,
       t2.cid_bind_bankcardno_3m_hf,
       t2.mobile_apply_3m_hf,
       t2.mobile_bind_bankcardno_3m_hf,
       t2.certno_bind_cid_3m_hf,
       t2.bankcardno_bind_cid_3m_hf,

       t3.certno_dist_cid_1m_am,
       t3.certno_dist_mobile_1m_am,
       t3.certno_dist_bankCardNo_1m_am,
       t3.certno_dist_bankCardMobile_1m_am,
       t3.certno_dist_deviceId_1m_am,
       t3.certno_dist_imei_1m_am,
       t3.certno_dist_idfa_1m_am,
       t3.cid_dist_bankCardNo_1m_am,
       t3.cid_dist_bankCardMobile_1m_am,
       t3.cid_dist_deviceId_1m_am,
       t3.cid_dist_idfa_1m_am,
       t3.cid_dist_imei_1m_am,
       t3.mobile_dist_cid_1m_am,
       t3.mobile_dist_certNo_1m_am,
       t3.mobile_dist_bankCardNo_1m_am,
       t3.mobile_dist_deviceId_1m_am,
       t3.mobile_dist_idfa_1m_am,
       t3.mobile_dist_imei_1m_am,
       t3.bankCardNo_dist_cid_1m_am,
       t3.bankCardNo_dist_mobile_1m_am,
       t3.bankCardNo_dist_deviceId_1m_am,
       t3.bankCardNo_dist_idfa_1m_am,
       t3.bankCardNo_dist_imei_1m_am,
       t3.bankCardMobile_dist_cid_1m_am,
       t3.bankCardMobile_dist_certNo_1m_am,
       t3.bankCardMobile_dist_bankCardNo_1m_am,
       t3.bankCardMobile_dist_deviceId_1m_am,
       t3.bankCardMobile_dist_idfa_1m_am,
       t3.bankCardMobile_dist_imei_1m_am,

       t4.certno_dist_cid_3m_am,
       t4.certno_dist_mobile_3m_am,
       t4.certno_dist_bankCardNo_3m_am,
       t4.certno_dist_bankCardMobile_3m_am,
       t4.certno_dist_deviceId_3m_am,
       t4.certno_dist_imei_3m_am,
       t4.certno_dist_idfa_3m_am,
       t4.cid_dist_bankCardNo_3m_am,
       t4.cid_dist_bankCardMobile_3m_am,
       t4.cid_dist_deviceId_3m_am,
       t4.cid_dist_idfa_3m_am,
       t4.cid_dist_imei_3m_am,
       t4.mobile_dist_cid_3m_am,
       t4.mobile_dist_certNo_3m_am,
       t4.mobile_dist_bankCardNo_3m_am,
       t4.mobile_dist_deviceId_3m_am,
       t4.mobile_dist_idfa_3m_am,
       t4.mobile_dist_imei_3m_am,
       t4.bankCardNo_dist_cid_3m_am,
       t4.bankCardNo_dist_mobile_3m_am,
       t4.bankCardNo_dist_deviceId_3m_am,
       t4.bankCardNo_dist_idfa_3m_am,
       t4.bankCardNo_dist_imei_3m_am,
       t4.bankCardMobile_dist_cid_3m_am,
       t4.bankCardMobile_dist_certNo_3m_am,
       t4.bankCardMobile_dist_bankCardNo_3m_am,
       t4.bankCardMobile_dist_deviceId_3m_am,
       t4.bankCardMobile_dist_idfa_3m_am,
       t4.bankCardMobile_dist_imei_3m_am,

       t5.ipcity_count_3m_abn,
       t5.ipcity_count_1m_abn,
       t5.ipcity_count_7d_abn,
       t5.ipcity_count_1d_abn,

       t6.event_certNo_flag,
       t6.event_residence_flag,
       t6.event_education_code,
       t6.event_organization_flag,
       t6.event_companyPhone_flag,
       t6.event_position_code,
       t6.event_workYears_code,
       t6.event_contactsName_flag,
       t6.event_contactsMobile_flag,
       t6.event_contactsRelationship_code,
       t6.event_bankCardType,
       t6.event_bankCardCode,
       t6.event_bankCardName_code,
       t6.event_srcChannel,
       t6.event_fpTokenID,
       t6.event_fingerprint,
       t6.event_ipCity_code,
       t6.event_mobileCity_code,
       t6.event_ipProvince_code,
       t6.event_longitude_flag,
       t6.event_latitude_flag,
       t6.event_ipLongitude_flag,
       t6.event_ipLatitude_flag,
       t6.event_gps_ip_longitude_latitude_flag,
       t6.event_loanAmount,
       t6.event_loanTerm,
       t6.event_productType,

       t7.register_count,
       t7.login_count,
       t7.app_apply_count,
       t7.app_bindcard_count,
       t7.app_resetpwd_count,
       t7.cid_dis_deviceid_1m,
       t7.cid_dis_event_imei_1m,
       t7.cid_dis_event_idfa_1m,
       t7.cid_dis_event_useragent_1m,
       t7.cid_dis_event_trueip_1m,
       t7.cid_dis_event_id_1m,
       t7.cid_dis_id_div_dis_trueip_1m,#--2
       t7.cid_dis_deviceid_3m,
       t7.cid_dis_event_imei_3m,
       t7.cid_dis_event_idfa_3m,
       t7.cid_dis_event_useragent_3m,
       t7.cid_dis_event_trueip_3m,
       t7.cid_dis_event_id_3m,
       t7.cid_dis_id_div_dis_trueip_3m,#--2
       t7.apply_ipcity_same_gpscity_flag,
       t7.apply_ipcity_same_mobilecity_flag,
       t7.apply_gpscity_same_mobilecity_flag,
       t7.trueip_dist_cid_1day,
       t7.trueip_dist_cid_7day,
       t7.trueip_dist_cid_1m,
       t7.trueip_dist_cid_3m,
       t7.mobile_same_bankcardmobile_flag,
       t7.loan_ipcity_same_mobilecity_flag,#--2
       t7.loan_first_without_apply_flag,#--2
       t7.loan_apply_time_interval_day,#--2
       t7.loan_apply_time_interval_second,#--2
       t7.loan_register_time_interval_day,#--2

       t8.collector_tstamp
from
    ml_lables t0 
right outer join cid_derived_hs_1m as t1 on t0.cid = t1.event_cid
left outer join cid_derived_hs_3m as t2 on t1.event_cid = t2.event_cid
left outer join cid_derived_1m_am_all as t3 on t1.event_cid = t3.event_cid
left outer join cid_derived_3m_am_all as t4 on t1.event_cid = t4.event_cid
left outer join cid_derived_abn_all as t5 on t1.event_cid = t5.event_cid
left outer join cid_first_loan_source_features as t6 on t1.event_cid = t6.event_cid
left outer join cid_new_feature_derived_all as t7 on t1.event_cid = t7.event_cid
left outer join cid_first_loan as t8 on t1.event_cid = t8.event_cid order by rand) t;

/*

select count(event_cid) from cid_derived_hs_1m;
select count(event_cid) from cid_derived_hs_3m;
select count(event_cid) from cid_derived_1m_am_all;
select count(event_cid) from cid_derived_3m_am_all;
select count(event_cid) from cid_derived_abn_all;
select count(event_cid) from cid_first_loan_source_features;
select count(event_cid) from cid_new_feature_derived_all;
select count(event_cid) from cid_first_loan;

*/


drop table if exists sinjor_train_data_rand_v3_b2;


create table sinjor_train_data_rand_v3_b2 as
select *
from xiaohua_feature_extract_all_v3
where cid < 70000;


drop table if exists sinjor_test_data_rand_v3_b2;


create table sinjor_test_data_rand_v3_b2 as
select *
from xiaohua_feature_extract_all_v3
where cid >= 70000;


mysql -h127.0.0.1 -uroot -proot -e"select * from sinjor_train_data_rand_v3_b2" xiaohua | sed -e "s/\t/,/g" -e "1s/[A-Z]/\l&/g" > "G:/mysql/MySQL Server 5.7/output/xiaohua/sinjor_train_data_rand_v3_b2.csv" 
mysql -h127.0.0.1 -uroot -proot -e"select * from sinjor_test_data_rand_v3_b2" xiaohua | sed -e "s/\t/,/g" -e "1s/[A-Z]/\l&/g" > "G:/mysql/MySQL Server 5.7/output/xiaohua/sinjor_test_data_rand_v3_b2.csv" 


hive -e "set hive.cli.print.header=true;select * from sinjor_db_time.sinjor_train_data_rand_v3;" | sed -e 's/sinjor_train_data_rand_v3.//g' -e 's/\t/,/g' >>sinjor_train_data_rand_v3.csv
hive -e "set hive.cli.print.header=true;select * from sinjor_db_time.sinjor_test_data_rand_v3;" | sed -e 's/sinjor_test_data_rand_v3.//g' -e 's/\t/,/g' >>sinjor_test_data_rand_v3.csv

hive -e "set hive.cli.print.header=true;select * from sinjor_db_time.sinjor_train_data_rand_v2;" | sed -e 's/sinjor_train_data_rand_v2.//g' -e 's/\t/,/g' >>sinjor_train_data_rand_v2.csv
hive -e "set hive.cli.print.header=true;select * from sinjor_db_time.sinjor_test_data_rand_v2;" | sed -e 's/sinjor_test_data_rand_v2.//g' -e 's/\t/,/g' >>sinjor_test_data_rand_v2.csv

hive -e "set hive.cli.print.header=true;select * from sinjor_db_time.sinjor_train_data_rand;" | sed -e 's/sinjor_train_data_rand.//g' -e 's/\t/,/g' >>sinjor_train_data_rand.csv
hive -e "set hive.cli.print.header=true;select * from sinjor_db_time.sinjor_test_data_rand;" | sed -e 's/sinjor_test_data_rand.//g' -e 's/\t/,/g' >>sinjor_test_data_rand.csv



--set source_database=sinjor_db_time;
--set label_database=default;
--set label_table=ml_labels;
--set train_table=sinjor_train_data_time;
--set test_table=sinjor_test_data_time;
--use ${hiveconf:source_database};
--

--
--drop table if exists ${hiveconf:train_table};
--
--create table ${hiveconf:train_table} as
--select *
--from xiaohua_feature_extract_all
--where collector_tstamp < "2017-10-01 00:00:00";
--
--drop table if exists ${hiveconf:test_table};
--
--create table ${hiveconf:test_table} as
--select *
--from xiaohua_feature_extract_all
--where collector_tstamp >= "2017-10-01 00:00:00";
--
--hive -e "set hive.cli.print.header=true;select * from sinjor_db_time.sinjor_train_data_time;" | sed -e 's/sinjor_train_data_time.//g' -e 's/\t/,/g' >>sinjor_train_data_time.csv
--hive -e "set hive.cli.print.header=true;select * from sinjor_db_time.sinjor_test_data_time;" | sed -e 's/sinjor_test_data_time.//g' -e 's/\t/,/g' >>sinjor_test_data_time.csv
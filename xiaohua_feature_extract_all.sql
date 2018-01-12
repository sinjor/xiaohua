--set source_database=sinjor_db_time;
--set label_database=default;
--set label_table=ml_labels;
--set train_table=sinjor_train_data_time;
--set test_table=sinjor_test_data_time;
--set feature_table=xiaohua_feature_extract_all;
use ${hiveconf:source_database};


drop table if exists ${hiveconf:feature_table};

create table ${hiveconf:feature_table} as
select row_number() over(order by rand()) as cid,*
from 
(select t0.cid as event_cid,
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
       t7.collector_tstamp
from
    ${hiveconf:label_database}.${hiveconf:label_table} t0 
right outer join cid_derived_hs_1m as t1 on t0.cid = t1.event_cid
left outer join cid_derived_hs_3m as t2 on t1.event_cid = t2.event_cid
left outer join cid_derived_1m_am_all as t3 on t1.event_cid = t3.event_cid
left outer join cid_derived_3m_am_all as t4 on t1.event_cid = t4.event_cid
left outer join cid_derived_abn_all as t5 on t1.event_cid = t5.event_cid
left outer join cid_first_loan_source_features as t6 on t1.event_cid = t6.event_cid
left outer join cid_first_loan as t7 on t1.event_cid = t7.event_cid) t;


set source_database=sinjor_db_rand;
set label_database=default;
set label_table=ml_labels;
set train_table=sinjor_train_data_rand;
set test_table=sinjor_test_data_rand;
use ${hiveconf:source_database};

drop table if exists ${hiveconf:train_table};


create table ${hiveconf:train_table} as
select *
from xiaohua_feature_extract_all
where cid < 70000;


drop table if exists ${hiveconf:test_table};


create table ${hiveconf:test_table} as
select *
from xiaohua_feature_extract_all
where cid >= 70000;

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
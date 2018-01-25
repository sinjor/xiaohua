--客户事件标示出现次数

drop table if exists cid_event_count_derived_all;


create table cid_event_count_derived_all as
select event_cid,
       max(case event_name
               when "register" then count
               else 0
           end) register_count,
       max(case event_name
               when "app_bindidcard" then count
               else 0
           end) app_bindidcard_count,
       max(case event_name
               when "app_resetpwd" then count
               else 0
           end) app_resetpwd_count,
       max(case event_name
               when "app_bindcard" then count
               else 0
           end) app_bindcard_count,
       max(case event_name
               when "app_apply" then count
               else 0
           end) app_apply_count,
       max(case event_name
               when "login" then count
               else 0
           end) login_count
from
    (select t1.event_cid,
            t2.event_name,
            count(*) as count
     from
         (select event_cid,
                 collector_tstamp
          from cid_first_loan) t1
     left outer join
         (select event_cid,
                 event_name,
                 collector_tstamp
          from behavior_data_source_useful_flatten_2) t2 on t1.event_cid = t2.event_cid
     where t1.collector_tstamp >=t2.collector_tstamp
     group by t1.event_cid,
              t2.event_name) t
group by event_cid;


--客户在loan之前使用的不同deviceid idfa ieme trueip event_id 个数
drop table if exists cid_dis_before_loan_derived_1m;


create table cid_dis_before_loan_derived_1m as
select t2.event_cid,
       count(distinct event_deviceid) as cid_dis_deviceid_1m,
       count(distinct event_imei) as cid_dis_event_imei_1m,
       count(distinct event_idfa) as cid_dis_event_idfa_1m,
       count(distinct event_useragent) as cid_dis_event_useragent_1m,
       count(distinct event_trueip) as cid_dis_event_trueip_1m,
       count(distinct event_id) as cid_dis_event_id_1m,
       count(distinct event_id)/count(distinct event_trueip) as cid_dis_id_div_dis_trueip_1m
from
    (select event_cid,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select event_cid,
            collector_tstamp,
            event_imei,
            event_idfa,
            event_deviceid,
            event_useragent,
            event_trueip,
            event_id
     from behavior_data_source_useful_flatten_2
     where collector_tstamp is not null
         and length(collector_tstamp) >0)t2 on t1.event_cid = t2.event_cid
where t1.collector_tstamp >= t2.collector_tstamp
    and date_sub(t1.collector_tstamp, INTERVAL 30 DAY) <= substring(t2.collector_tstamp, 1,10)
group by t2.event_cid;


drop table if exists cid_dis_before_loan_derived_3m;


create table cid_dis_before_loan_derived_3m as
select t2.event_cid,
       count(distinct event_deviceid) as cid_dis_deviceid_3m,
       count(distinct event_imei) as cid_dis_event_imei_3m,
       count(distinct event_idfa) as cid_dis_event_idfa_3m,
       count(distinct event_useragent) as cid_dis_event_useragent_3m,
       count(distinct event_trueip) as cid_dis_event_trueip_3m,
       count(distinct event_id) as cid_dis_event_id_3m,
       count(distinct event_id)/count(distinct event_trueip) as cid_dis_id_div_dis_trueip_3m
from
    (select event_cid,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select event_cid,
            collector_tstamp,
            event_imei,
            event_idfa,
            event_deviceid,
            event_useragent,
            event_trueip,
            event_id
     from behavior_data_source_useful_flatten_2
     where collector_tstamp is not null
         and length(collector_tstamp) >0)t2 on t1.event_cid = t2.event_cid
where t1.collector_tstamp >= t2.collector_tstamp
    and date_sub(t1.collector_tstamp, interval 90 day) <= substring(t2.collector_tstamp, 1,10)
group by t2.event_cid;

--apply手机号城市和ip城市是否相等
--apply gps城市和ip城市是否相等
--apply gps城市和手机号城市是否相等

drop table if exists cid_apply_city_same_derived_all;


create table cid_apply_city_same_derived_all as
select event_gpsCity,
       event_ipcity,
       event_mobileCity,
       event_cid,
       case
           when event_ipcity = event_gpsCity
                and event_ipcity is not null
                and length(event_ipcity) > 0
                and event_gpsCity is not null
                and length(event_gpsCity) > 0 then 2
           when event_ipcity != event_gpsCity
                and event_ipcity is not null
                and length(event_ipcity) > 0
                and event_gpsCity is not null
                and length(event_gpsCity) > 0 then 1
           else 0
       end as apply_ipcity_same_gpscity_flag,
       case
           when substring_index(event_mobileCity, event_ipcity, 1) = event_mobileCity
                and event_ipcity is not null
                and length(event_ipcity) > 0
                and event_mobileCity is not null
                and length(event_mobileCity) > 0 then 1
           when substring_index(event_mobileCity, event_ipcity, 1) != event_mobileCity
                and event_ipcity is not null
                and length(event_ipcity) > 0
                and event_mobileCity is not null
                and length(event_mobileCity) > 0 then 2
           else 0
       end as apply_ipcity_same_mobilecity_flag,
       case
           when substring_index(event_mobileCity, event_gpscity, 1) = event_mobileCity
                and event_gpscity is not null
                and length(event_gpscity) > 0
                and event_mobileCity is not null
                and length(event_mobileCity) > 0 then 1
           when substring_index(event_mobileCity, event_gpscity, 1) != event_mobileCity
                and event_gpscity is not null
                and length(event_gpscity) > 0
                and event_mobileCity is not null
                and length(event_mobileCity) > 0 then 2
           else 0
       end as apply_gpscity_same_mobilecity_flag
from cid_last_apply_before_loan;

--在loan前的ip对应存在的不同客户个数（1天内，7天内 一月内,三月内）
drop table if exists trueip_dist_cid_derived_1day;


create table trueip_dist_cid_derived_1day as
select t3.event_cid,
       t4.trueip_dist_cid_1day
from
    (select event_cid,
            collector_tstamp,
            event_trueip
     from cid_first_loan) t3
left outer join
    (select t1.event_trueip,
            t1.collector_tstamp,
            count(distinct t2.event_cid) as trueip_dist_cid_1day
     from
         (select event_cid,
                 collector_tstamp,
                 event_trueip
          from cid_first_loan) t1
     left outer join
         (select event_cid,
                 event_trueip,
                 collector_tstamp
          from behavior_data_source_useful_flatten_2
          where collector_tstamp is not null
              and length(collector_tstamp) > 0
              and event_trueip is not null
              and length(event_trueip)>0 ) t2 on t1.event_trueip = t2.event_trueip
     where t1.collector_tstamp >t2.collector_tstamp
         and (date_sub(t1.collector_tstamp, interval 1 day) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_trueip,
              t1.collector_tstamp) t4 on t3.event_trueip = t4.event_trueip
and t3.collector_tstamp = t4.collector_tstamp ;


drop table if exists trueip_dist_cid_derived_7day;


create table trueip_dist_cid_derived_7day as
select t3.event_cid,
       t4.trueip_dist_cid_7day
from
    (select event_cid,
            collector_tstamp,
            event_trueip
     from cid_first_loan) t3
left outer join
    (select t1.event_trueip,
            t1.collector_tstamp,
            count(distinct t2.event_cid) as trueip_dist_cid_7day
     from
         (select event_cid,
                 collector_tstamp,
                 event_trueip
          from cid_first_loan) t1
     left outer join
         (select event_cid,
                 event_trueip,
                 collector_tstamp
          from behavior_data_source_useful_flatten_2
          where collector_tstamp is not null
              and length(collector_tstamp) > 0
              and event_trueip is not null
              and length(event_trueip)>0 ) t2 on t1.event_trueip = t2.event_trueip
     where t1.collector_tstamp >t2.collector_tstamp
         and (date_sub(t1.collector_tstamp,interval 7 day) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_trueip,
              t1.collector_tstamp) t4 on t3.event_trueip = t4.event_trueip
and t3.collector_tstamp = t4.collector_tstamp ;


drop table if exists trueip_dist_cid_derived_1m;


create table trueip_dist_cid_derived_1m as
select t3.event_cid,
       t4.trueip_dist_cid_1m
from
    (select event_cid,
            collector_tstamp,
            event_trueip
     from cid_first_loan) t3
left outer join
    (select t1.event_trueip,
            t1.collector_tstamp,
            count(distinct t2.event_cid) as trueip_dist_cid_1m
     from
         (select event_cid,
                 collector_tstamp,
                 event_trueip
          from cid_first_loan) t1
     left outer join
         (select event_cid,
                 event_trueip,
                 collector_tstamp
          from behavior_data_source_useful_flatten_2
          where collector_tstamp is not null
              and length(collector_tstamp) > 0
              and event_trueip is not null
              and length(event_trueip)>0 ) t2 on t1.event_trueip = t2.event_trueip
     where t1.collector_tstamp >t2.collector_tstamp
         and (date_sub(t1.collector_tstamp,interval 30 day) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_trueip,
              t1.collector_tstamp) t4 on t3.event_trueip = t4.event_trueip
and t3.collector_tstamp = t4.collector_tstamp ;


drop table if exists trueip_dist_cid_derived_3m;


create table trueip_dist_cid_derived_3m as
select t3.event_cid,
       t4.trueip_dist_cid_3m
from
    (select event_cid,
            collector_tstamp,
            event_trueip
     from cid_first_loan) t3
left outer join
    (select t1.event_trueip,
            t1.collector_tstamp,
            count(distinct t2.event_cid) as trueip_dist_cid_3m
     from
         (select event_cid,
                 collector_tstamp,
                 event_trueip
          from cid_first_loan) t1
     left outer join
         (select event_cid,
                 event_trueip,
                 collector_tstamp
          from behavior_data_source_useful_flatten_2
          where collector_tstamp is not null
              and length(collector_tstamp) > 0
              and event_trueip is not null
              and length(event_trueip)>0 ) t2 on t1.event_trueip = t2.event_trueip
     where t1.collector_tstamp >t2.collector_tstamp
         and (date_sub(t1.collector_tstamp,interval 90 day) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_trueip,
              t1.collector_tstamp) t4 on t3.event_trueip = t4.event_trueip
and t3.collector_tstamp = t4.collector_tstamp ;


drop table if exists cid_loan_derived_all;


create table cid_loan_derived_all as
select t1.event_cid,
       case
           when t1.event_mobile is not null
                and length(t1.event_mobile) >0
                and t1.event_bankcardmobile is not null
                and length(t1.event_bankcardmobile) >0
                and t1.event_mobile = t1.event_bankcardmobile then 1
           else 0
       end as mobile_same_bankcardmobile_flag,
       case
           when substring_index(t1.event_mobileCity, t1.event_ipcity, 1) = t1.event_mobileCity
                and t1.event_ipcity is not null
                and length(t1.event_ipcity) > 0
                and t1.event_mobileCity is not null
                and length(t1.event_mobileCity) > 0 then 1
           when substring_index(t1.event_mobileCity, t1.event_ipcity, 1) != t1.event_mobileCity
                and t1.event_ipcity is not null
                and length(t1.event_ipcity) > 0
                and t1.event_mobileCity is not null
                and length(t1.event_mobileCity) > 0 then 2
           else 0
       end as loan_ipcity_same_mobilecity_flag,
       case
           when t2.event_cid is not null then 1
           else 0
       end as loan_first_without_apply_flag,
       case when t2.event_cid is not null then datediff(t2.loan_collector_tstamp, t2.collector_tstamp)
       else null
       end as loan_apply_time_interval_day,
       case
           when t2.event_cid is not null then cast((unix_timestamp(t2.loan_collector_tstamp)-unix_timestamp(t2.collector_tstamp))/60 as signed)
           else null
       end as loan_apply_time_interval_second
from cid_first_loan t1
left outer join cid_last_apply_before_loan t2 on t1.event_cid = t2.event_cid;


--40.设备id对应的身份证号
--41.设备id对应的客户号
--42.设备id对应的微信
--43.设备id对应的手机号
--44.设备id对应的银行卡
--45.设备id对应的银行预留手机号
/*
drop table if exists apply_deviceid_derived_1m;


create table apply_deviceid_derived_1m as
select t3.event_cid,
       t4.apply_deviceid_dist_cid_1m,
       t4.apply_deviceid_dist_certno_1m,
       t4.apply_deviceid_dist_mobile_1m,
       t4.apply_deviceid_dist_bankcardno_1m,
       t4.apply_deviceid_dist_bankcardmobile_1m
from
    (select event_cid,
            collector_tstamp,
            event_deviceid
     from cid_last_apply_before_loan) t3
left outer join
    (select t1.event_deviceid,
            t1.loan_collector_tstamp,
            count(distinct t2.event_cid) as apply_deviceid_dist_cid_1m,
            count(distinct t2.event_certno) as apply_deviceid_dist_certno_1m,
            count(distinct t2.event_mobile) as apply_deviceid_dist_mobile_1m,
            count(distinct t2.event_bankcardno) as apply_deviceid_dist_bankcardno_1m,
            count(distinct t2.event_bankcardmobile) as apply_deviceid_dist_bankcardmobile_1m
     from
         (select event_cid,
                 loan_collector_tstamp,
                 event_deviceid
          from cid_last_apply_before_loan ) t1
     left outer join
         (select distinct event_deviceid,
                 collector_tstamp,
                 event_cid,
                 event_certno,
                 event_mobile,
                 event_bankcardno,
                 event_bankcardmobile
          from behavior_data_source_useful_flatten_2
          where collector_tstamp is not null
              and length(collector_tstamp) > 0
              and event_deviceid is not null
              and length(event_deviceid)>0 ) t2 on t1.event_deviceid = t2.event_deviceid
     where t1.loan_collector_tstamp >t2.collector_tstamp
         and (date_sub(t1.loan_collector_tstamp,30) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_deviceid,
              t1.loan_collector_tstamp) t4 on t3.event_deviceid = t4.event_deviceid
and t3.collector_tstamp = t4.loan_collector_tstamp ;

drop table if exists apply_deviceid_derived_3m;


create table apply_deviceid_derived_3m as
select t3.event_cid,
       t4.apply_deviceid_dist_cid_3m,
       t4.apply_deviceid_dist_certno_3m,
       t4.apply_deviceid_dist_mobile_3m,
       t4.apply_deviceid_dist_bankcardno_3m,
       t4.apply_deviceid_dist_bankcardmobile_3m
from
    (select event_cid,
            collector_tstamp,
            event_deviceid
     from cid_last_apply_before_loan) t3
left outer join
    (select t1.event_deviceid,
            t1.loan_collector_tstamp,
            count(distinct t2.event_cid) as apply_deviceid_dist_cid_3m,
            count(distinct t2.event_certno) as apply_deviceid_dist_certno_3m,
            count(distinct t2.event_mobile) as apply_deviceid_dist_mobile_3m,
            count(distinct t2.event_bankcardno) as apply_deviceid_dist_bankcardno_3m,
            count(distinct t2.event_bankcardmobile) as apply_deviceid_dist_bankcardmobile_3m
     from
         (select event_cid,
                 loan_collector_tstamp,
                 event_deviceid
          from cid_last_apply_before_loan) t1
     left outer join
         (select event_deviceid,
                 collector_tstamp,
                 event_cid,
                 event_certno,
                 event_mobile,
                 event_bankcardno,
                 event_bankcardmobile
          from behavior_data_source_useful_flatten_2
          where collector_tstamp is not null
              and length(collector_tstamp) > 0
              and event_deviceid is not null
              and length(event_deviceid)>0 ) t2 on t1.event_deviceid = t2.event_deviceid
     where t1.loan_collector_tstamp >t2.collector_tstamp
         and (date_sub(t1.loan_collector_tstamp,90) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_deviceid,
              t1.loan_collector_tstamp) t4 on t3.event_deviceid = t4.event_deviceid
and t3.collector_tstamp = t4.loan_collector_tstamp ;
*/
--客户注册与首次贷款的时间间隔 单位天


drop table if exists loan_register_time_interval_day_derived_all;


create table loan_register_time_interval_day_derived_all as
select t1.event_cid,
       t1.collector_tstamp,
       t2.collector_tstamp_first_register,
       case
           when t2.collector_tstamp_first_register is not null then datediff(t1.collector_tstamp, t2.collector_tstamp_first_register)
           else null
       end as loan_register_time_interval_day
from
    (select event_cid,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select event_cid,
            min(collector_tstamp) as collector_tstamp_first_register
     from behavior_data_source_useful_flatten_2
     where event_name = "register"
     group by event_cid) t2 on t1.event_cid = t2.event_cid;




alter table cid_event_count_derived_all add index index_event_cid (event_cid);
alter table cid_dis_before_loan_derived_1m add index index_event_cid (event_cid);
alter table cid_dis_before_loan_derived_3m add index index_event_cid (event_cid);
alter table cid_apply_city_same_derived_all add index index_event_cid (event_cid);
alter table trueip_dist_cid_derived_1day add index index_event_cid (event_cid);
alter table trueip_dist_cid_derived_7day add index index_event_cid (event_cid);
alter table trueip_dist_cid_derived_1m add index index_event_cid (event_cid);
alter table trueip_dist_cid_derived_3m add index index_event_cid (event_cid);
alter table cid_loan_derived_all add index index_event_cid (event_cid);
alter table loan_register_time_interval_day_derived_all add index index_event_cid (event_cid);


drop table cid_new_feature_derived_all;


create table cid_new_feature_derived_all as
select t0.event_cid,
       t1.register_count,
       t1.login_count,
       t1.app_apply_count,
       t1.app_bindcard_count,
       t1.app_resetpwd_count,
       t2.cid_dis_deviceid_1m,
       t2.cid_dis_event_imei_1m,
       t2.cid_dis_event_idfa_1m,
       t2.cid_dis_event_useragent_1m,
       t2.cid_dis_event_trueip_1m,
       t2.cid_dis_event_id_1m,
       t2.cid_dis_id_div_dis_trueip_1m,#--2
       t3.cid_dis_deviceid_3m,
       t3.cid_dis_event_imei_3m,
       t3.cid_dis_event_idfa_3m,
       t3.cid_dis_event_useragent_3m,
       t3.cid_dis_event_trueip_3m,
       t3.cid_dis_event_id_3m,
       t3.cid_dis_id_div_dis_trueip_3m,#--2
       t4.apply_ipcity_same_gpscity_flag,
       t4.apply_ipcity_same_mobilecity_flag,
       t4.apply_gpscity_same_mobilecity_flag,
       t5.trueip_dist_cid_1day,
       t6.trueip_dist_cid_7day,
       t7.trueip_dist_cid_1m,
       t8.trueip_dist_cid_3m,
       t9.mobile_same_bankcardmobile_flag,
       t9.loan_ipcity_same_mobilecity_flag,#--2
       t9.loan_first_without_apply_flag,#--2
       t9.loan_apply_time_interval_day,#--2
       t9.loan_apply_time_interval_second,#--2
       t10.loan_register_time_interval_day#--2
from
    (select event_cid
     from cid_first_loan) t0
left outer join cid_event_count_derived_all as t1 on t0.event_cid = t1.event_cid
left outer join cid_dis_before_loan_derived_1m as t2 on t0.event_cid = t2.event_cid
left outer join cid_dis_before_loan_derived_3m as t3 on t0.event_cid = t3.event_cid
left outer join cid_apply_city_same_derived_all as t4 on t0.event_cid = t4.event_cid
left outer join trueip_dist_cid_derived_1day as t5 on t0.event_cid = t5.event_cid
left outer join trueip_dist_cid_derived_7day as t6 on t0.event_cid = t6.event_cid
left outer join trueip_dist_cid_derived_1m as t7 on t0.event_cid = t7.event_cid
left outer join trueip_dist_cid_derived_3m as t8 on t0.event_cid = t8.event_cid
left outer join cid_loan_derived_all as t9 on t0.event_cid = t9.event_cid
left outer join loan_register_time_interval_day_derived_all as t10 on t0.event_cid = t10.event_cid;




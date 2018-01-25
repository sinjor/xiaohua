/*
1.身份证号对应的客户号
2.身份证号对应的注册手机号
3.身份证号对应的绑定银行卡
4.身份证号对应的银行卡预留手机号
5.身份证号对应的设备id
6.身份证号对应的imei
7.身份证号对应的idfa
 */
drop table if exists cid_certNo_derived_1m ;


create table cid_certNo_derived_1m as
select t1.event_cid,
       t1.event_certNo,
       t2.certno_dist_cid_1m_am,
       t2.certno_dist_mobile_1m_am,
       t2.certno_dist_bankCardNo_1m_am,
       t2.certno_dist_bankCardMobile_1m_am,
       t2.certno_dist_deviceId_1m_am,
       t2.certno_dist_imei_1m_am,
       t2.certno_dist_idfa_1m_am
from
    (select event_cid,
            event_certNo,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select t3.event_certNo,
            t3.collector_tstamp,
            count(distinct t4.event_cid) as certno_dist_cid_1m_am,
            count(distinct t4.event_mobile) as certno_dist_mobile_1m_am,
            count(distinct t4.event_bankCardNo) as certno_dist_bankCardNo_1m_am,
            count(distinct t4.event_bankCardMobile) as certno_dist_bankCardMobile_1m_am,
            count(distinct t4.event_deviceId) as certno_dist_deviceId_1m_am,
            count(distinct t4.event_imei) as certno_dist_imei_1m_am,
            count(distinct t4.event_idfa) as certno_dist_idfa_1m_am
     from
         (select event_cid,
                 event_certNo,
                 collector_tstamp
          from cid_first_loan) t3
     left outer join
         (select distinct event_certNo,
                          collector_tstamp,
                          event_cid,
                          event_mobile,
                          event_bankCardNo,
                          event_bankCardMobile,
                          event_deviceId,
                          event_imei,
                          event_idfa
          from behavior_data_source_useful_flatten_2
          where event_certNo is not null
              and length(event_certNo) > 0
              and collector_tstamp is not null
              and length(collector_tstamp) > 0) t4 on t3.event_certNo = t4.event_certNo
     where (t3.collector_tstamp >= t4.collector_tstamp)
         and (date_sub(t3.collector_tstamp,INTERVAL 30 DAY) <= substring(t4.collector_tstamp, 1,10))
     group by t3.event_certNo,
              t3.collector_tstamp)t2 on t1.event_certNo = t2.event_certNo
and t1.collector_tstamp = t2.collector_tstamp;

/*

 8.客户号对应绑定银行卡
9.客户号对应的银行预留手机号
10.客户号对应的设备id
11.客户号对应的imei地址
12.客户号对应的idfa地址

 */
drop table if exists cid_derived_1m ;


create table cid_derived_1m as
select t1.event_cid,
       count(distinct t2.event_bankCardNo) as cid_dist_bankCardNo_1m_am,
       count(distinct t2.event_bankCardMobile) as cid_dist_bankCardMobile_1m_am,
       count(distinct t2.event_deviceId) as cid_dist_deviceId_1m_am,
       count(distinct t2.event_imei) as cid_dist_imei_1m_am,
       count(distinct t2.event_idfa) as cid_dist_idfa_1m_am
from
    (select event_cid,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select distinct event_cid,
                     collector_tstamp,
                     event_mobile,
                     event_bankCardNo,
                     event_bankCardMobile,
                     event_deviceId,
                     event_imei,
                     event_idfa
     from behavior_data_source_useful_flatten_2
     where event_cid is not null
         and length(event_cid) > 0
         and collector_tstamp is not null
         and length(collector_tstamp) > 0) t2 on t1.event_cid = t2.event_cid
where (t1.collector_tstamp >= t2.collector_tstamp)
    and (date_sub(t1.collector_tstamp,INTERVAL 30 DAY) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid ;

/*
20.手机号对应的身份证号
21.手机号对应的客户号
/////////22.手机号对应的绑定微信
23.手机号对应的绑定银行卡
24.手机号对应的设备id
25.手机号对应的imei地址
26.手机号对应的idfa地址
 */
drop table if exists cid_mobile_derived_1m ;


create table cid_mobile_derived_1m as
select t1.event_cid,
       t1.event_mobile,
       t2.mobile_dist_certNo_1m_am,
       t2.mobile_dist_cid_1m_am,
       t2.mobile_dist_bankCardNo_1m_am,
       t2.mobile_dist_deviceId_1m_am,
       t2.mobile_dist_imei_1m_am,
       t2.mobile_dist_idfa_1m_am
from
    (select event_cid,
            event_mobile,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select t3.event_mobile,
            t3.collector_tstamp,
            count(distinct t4.event_certNo) as mobile_dist_certNo_1m_am,
            count(distinct t4.event_cid) as mobile_dist_cid_1m_am,
            count(distinct t4.event_bankCardNo) as mobile_dist_bankCardNo_1m_am,
            count(distinct t4.event_deviceId) as mobile_dist_deviceId_1m_am,
            count(distinct t4.event_imei) as mobile_dist_imei_1m_am,
            count(distinct t4.event_idfa) as mobile_dist_idfa_1m_am
     from
         (select event_cid,
                 event_mobile,
                 collector_tstamp
          from cid_first_loan) t3
     left outer join
         (select distinct event_mobile,
                          collector_tstamp,
                          event_certNo,
                          event_cid,
                          event_bankCardNo,
                          event_deviceId,
                          event_imei,
                          event_idfa
          from behavior_data_source_useful_flatten_2
          where event_mobile is not null
              and length(event_mobile) > 0
              and collector_tstamp is not null
              and length(collector_tstamp) > 0) t4 on t3.event_mobile = t4.event_mobile
     where (t3.collector_tstamp >= t4.collector_tstamp)
         and (date_sub(t3.collector_tstamp,INTERVAL 30 DAY) <= substring(t4.collector_tstamp, 1,10))
     group by t3.event_mobile,
              t3.collector_tstamp)t2 on t1.event_mobile = t2.event_mobile
and t1.collector_tstamp = t2.collector_tstamp ;

/*
27.银行卡对应的客户号
////28.银行卡对应的微信
29.银行卡对应的手机号
INTERVAL 30 DAY.银行卡对应的设备id
31.银行卡对应的imei地址
32.银行卡对应的idfa地址
*/
drop table if exists cid_bankCardNo_derived_1m ;


create table cid_bankCardNo_derived_1m as
select t1.event_cid,
       t1.event_bankCardNo,
       t2.bankCardNo_dist_cid_1m_am,
       t2.bankCardNo_dist_mobile_1m_am,
       t2.bankCardNo_dist_deviceId_1m_am,
       t2.bankCardNo_dist_imei_1m_am,
       t2.bankCardNo_dist_idfa_1m_am
from
    (select event_cid,
            event_bankCardNo,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select t3.collector_tstamp,
            t3.event_bankCardNo,
            count(distinct t4.event_cid) as bankCardNo_dist_cid_1m_am,
            count(distinct t4.event_mobile) as bankCardNo_dist_mobile_1m_am,
            count(distinct t4.event_deviceId) as bankCardNo_dist_deviceId_1m_am,
            count(distinct t4.event_imei) as bankCardNo_dist_imei_1m_am,
            count(distinct t4.event_idfa) as bankCardNo_dist_idfa_1m_am
     from
         (select event_cid,
                 event_bankCardNo,
                 collector_tstamp
          from cid_first_loan) t3
     left outer join
         (select distinct event_bankCardNo,
                          collector_tstamp,
                          event_cid,
                          event_mobile,
                          event_deviceId,
                          event_imei,
                          event_idfa
          from behavior_data_source_useful_flatten_2
          where event_bankCardNo is not null
              and length(event_bankCardNo) > 0
              and collector_tstamp is not null
              and length(collector_tstamp) > 0) t4 on t3.event_bankCardNo = t4.event_bankCardNo
     where (t3.collector_tstamp >= t4.collector_tstamp)
         and (date_sub(t3.collector_tstamp,INTERVAL 30 DAY) <= substring(t4.collector_tstamp, 1,10))
     group by t3.event_bankCardNo,
              t3.collector_tstamp)t2 on t1.event_bankCardNo = t2.event_bankCardNo
and t1.collector_tstamp = t2.collector_tstamp;

/*
33.银行预留手机号对应的身份证
34.银行预留手机号对应的客户号
///////35.银行预留手机号对应的微信
36.银行预留手机号对应的银行卡
37.银行预留手机号对应的设备id
38.银行预留手机号对应的imei地址
39.银行预留手机号对应的idfa地址

 */
drop table if exists cid_bankCardMobile_derived_1m ;


create table cid_bankCardMobile_derived_1m as
select t1.event_cid,
       t1.event_bankCardMobile,
       t2.bankCardMobile_dist_certNo_1m_am,
       t2.bankCardMobile_dist_cid_1m_am,
       t2.bankCardMobile_dist_bankCardNo_1m_am,
       t2.bankCardMobile_dist_deviceId_1m_am,
       t2.bankCardMobile_dist_imei_1m_am,
       t2.bankCardMobile_dist_idfa_1m_am
from
    (select event_cid,
            event_bankCardMobile,
            collector_tstamp
     from cid_first_loan) t1
left outer join
(select t3.event_bankCardMobile,
       t3.collector_tstamp,
       count(distinct t4.event_certNo) as bankCardMobile_dist_certNo_1m_am,
       count(distinct t4.event_cid) as bankCardMobile_dist_cid_1m_am,
       count(distinct t4.event_bankCardNo) as bankCardMobile_dist_bankCardNo_1m_am,
       count(distinct t4.event_deviceId) as bankCardMobile_dist_deviceId_1m_am,
       count(distinct t4.event_imei) as bankCardMobile_dist_imei_1m_am,
       count(distinct t4.event_idfa) as bankCardMobile_dist_idfa_1m_am
from
    (select event_cid,
            event_bankCardMobile,
            collector_tstamp
     from cid_first_loan) t3
left outer join
    (select distinct event_bankCardMobile,
                     collector_tstamp,
                     event_certNo,
                     event_cid,
                     event_bankCardNo,
                     event_deviceId,
                     event_imei,
                     event_idfa
     from behavior_data_source_useful_flatten_2
     where event_bankCardMobile is not null
         and length(event_bankCardMobile) > 0
         and collector_tstamp is not null
         and length(collector_tstamp) > 0) t4 on t3.event_bankCardMobile = t4.event_bankCardMobile
where (t3.collector_tstamp >= t4.collector_tstamp)
    and (date_sub(t3.collector_tstamp,INTERVAL 30 DAY) <= substring(t4.collector_tstamp, 1,10))
group by t3.event_bankCardMobile,
         t3.collector_tstamp)t2 on t1.event_bankCardMobile = t2.event_bankCardMobile
and t1.collector_tstamp = t2.collector_tstamp ;


alter table cid_certNo_derived_1m add index index_event_cid (event_cid);
alter table cid_derived_1m add index index_event_cid (event_cid);
alter table cid_mobile_derived_1m add index index_event_cid (event_cid);
alter table cid_bankCardNo_derived_1m add index index_event_cid (event_cid);
alter table cid_bankCardMobile_derived_1m add index index_event_cid (event_cid);

drop table if exists cid_derived_1m_am_all ;

create table cid_derived_1m_am_all as
select t0.event_cid,
       t1.certno_dist_cid_1m_am,
       t1.certno_dist_mobile_1m_am,
       t1.certno_dist_bankCardNo_1m_am,
       t1.certno_dist_bankCardMobile_1m_am,
       t1.certno_dist_deviceId_1m_am,
       t1.certno_dist_imei_1m_am,
       t1.certno_dist_idfa_1m_am,
       t2.cid_dist_bankCardNo_1m_am,
       t2.cid_dist_bankCardMobile_1m_am,
       t2.cid_dist_deviceId_1m_am,
       t2.cid_dist_idfa_1m_am,
       t2.cid_dist_imei_1m_am,
       t3.mobile_dist_cid_1m_am,
       t3.mobile_dist_certNo_1m_am,
       t3.mobile_dist_bankCardNo_1m_am,
       t3.mobile_dist_deviceId_1m_am,
       t3.mobile_dist_idfa_1m_am,
       t3.mobile_dist_imei_1m_am,
       t4.bankCardNo_dist_cid_1m_am,
       t4.bankCardNo_dist_mobile_1m_am,
       t4.bankCardNo_dist_deviceId_1m_am,
       t4.bankCardNo_dist_idfa_1m_am,
       t4.bankCardNo_dist_imei_1m_am,
       t5.bankCardMobile_dist_cid_1m_am,
       t5.bankCardMobile_dist_certNo_1m_am,
       t5.bankCardMobile_dist_bankCardNo_1m_am,
       t5.bankCardMobile_dist_deviceId_1m_am,
       t5.bankCardMobile_dist_idfa_1m_am,
       t5.bankCardMobile_dist_imei_1m_am
from
    (select event_cid
     from cid_first_loan) t0
left outer join cid_certNo_derived_1m as t1 on t0.event_cid = t1.event_cid
left outer join cid_derived_1m as t2 on t0.event_cid = t2.event_cid
left outer join cid_mobile_derived_1m as t3 on t0.event_cid = t3.event_cid
left outer join cid_bankCardNo_derived_1m as t4 on t0.event_cid = t4.event_cid
left outer join cid_bankCardMobile_derived_1m as t5 on t0.event_cid = t5.event_cid;




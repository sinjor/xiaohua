 /*
1.身份证号对应的客户号
2.身份证号对应的注册手机号
3.身份证号对应的绑定银行卡
4.身份证号对应的银行卡预留手机号
5.身份证号对应的设备id
6.身份证号对应的imei
7.身份证号对应的idfa
 */
drop table if exists cid_certNo_derived_3m ;


create table cid_certNo_derived_3m as
select t1.event_cid,
       t1.event_certNo,
       size(collect_set(t2.event_cid)) as certno_dist_cid_3m_am,
       size(collect_set(t2.event_mobile)) as certno_dist_mobile_3m_am,
       size(collect_set(t2.event_bankCardNo)) as certno_dist_bankCardNo_3m_am,
       size(collect_set(t2.event_bankCardMobile)) as certno_dist_bankCardMobile_3m_am,
       size(collect_set(t2.event_deviceId)) as certno_dist_deviceId_3m_am,
       size(collect_set(t2.event_imei)) as certno_dist_imei_3m_am,
       size(collect_set(t2.event_idfa)) as certno_dist_idfa_3m_am
from
    (select event_cid,
            event_certNo,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select distinct event_certNo,
                     collector_tstamp,
                     event_cid ,
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
         and length(collector_tstamp) > 0) t2 on t1.event_certNo = t2.event_certNo
where (t1.collector_tstamp >= t2.collector_tstamp)
    and (date_sub(t1.collector_tstamp,90) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid,
         t1.event_certNo ;

 /*

 8.客户号对应绑定银行卡
9.客户号对应的银行预留手机号
10.客户号对应的设备id
11.客户号对应的imei地址
12.客户号对应的idfa地址

 */
drop table if exists cid_derived_3m ;


create table cid_derived_3m as
select t1.event_cid,
       size(collect_set(t2.event_bankCardNo)) as cid_dist_bankCardNo_3m_am,
       size(collect_set(t2.event_bankCardMobile)) as cid_dist_bankCardMobile_3m_am,
       size(collect_set(t2.event_deviceId)) as cid_dist_deviceId_3m_am,
       size(collect_set(t2.event_imei)) as cid_dist_imei_3m_am,
       size(collect_set(t2.event_idfa)) as cid_dist_idfa_3m_am
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
    and (date_sub(t1.collector_tstamp,90) <= substring(t2.collector_tstamp, 1,10))
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
drop table if exists cid_mobile_derived_3m ;


create table cid_mobile_derived_3m as
select t1.event_cid,
       t1.event_mobile,
       size(collect_set(t2.event_certNo)) as mobile_dist_certNo_3m_am,
       size(collect_set(t2.event_cid)) as mobile_dist_cid_3m_am,
       size(collect_set(t2.event_bankCardNo)) as mobile_dist_bankCardNo_3m_am,
       size(collect_set(t2.event_deviceId)) as mobile_dist_deviceId_3m_am,
       size(collect_set(t2.event_imei)) as mobile_dist_imei_3m_am,
       size(collect_set(t2.event_idfa)) as mobile_dist_idfa_3m_am
from
    (select event_cid,
            event_mobile,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select distinct event_mobile,
                     collector_tstamp,
                     event_certNo,
                     event_cid ,
                     event_bankCardNo,
                     event_deviceId,
                     event_imei,
                     event_idfa
     from behavior_data_source_useful_flatten_2
     where event_mobile is not null
         and length(event_mobile) > 0
         and collector_tstamp is not null
         and length(collector_tstamp) > 0) t2 on t1.event_mobile = t2.event_mobile
where (t1.collector_tstamp >= t2.collector_tstamp)
    and (date_sub(t1.collector_tstamp,90) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid,
         t1.event_mobile ;

 /*
27.银行卡对应的客户号
////28.银行卡对应的微信
29.银行卡对应的手机号
30.银行卡对应的设备id
31.银行卡对应的imei地址
32.银行卡对应的idfa地址
*/
drop table if exists cid_bankCardNo_derived_3m ;


create table cid_bankCardNo_derived_3m as
select t1.event_cid,
       t1.event_bankCardNo,
       size(collect_set(t2.event_cid)) as bankCardNo_dist_cid_3m_am,
       size(collect_set(t2.event_mobile)) as bankCardNo_dist_mobile_3m_am,
       size(collect_set(t2.event_deviceId)) as bankCardNo_dist_deviceId_3m_am,
       size(collect_set(t2.event_imei)) as bankCardNo_dist_imei_3m_am,
       size(collect_set(t2.event_idfa)) as bankCardNo_dist_idfa_3m_am
from
    (select event_cid,
            event_bankCardNo,
            collector_tstamp
     from cid_first_loan2) t1
left outer join
    (select distinct event_bankCardNo,
                     collector_tstamp,
                     event_cid ,
                     event_mobile,
                     event_deviceId,
                     event_imei,
                     event_idfa
     from behavior_data_source_useful_flatten_2
     where event_bankCardNo is not null
         and length(event_bankCardNo) > 0
         and collector_tstamp is not null
         and length(collector_tstamp) > 0) t2 on t1.event_bankCardNo = t2.event_bankCardNo
where (t1.collector_tstamp >= t2.collector_tstamp)
    and (date_sub(t1.collector_tstamp,90) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid,
         t1.event_bankCardNo ;

 /*
33.银行预留手机号对应的身份证
34.银行预留手机号对应的客户号
///////35.银行预留手机号对应的微信
36.银行预留手机号对应的银行卡
37.银行预留手机号对应的设备id
38.银行预留手机号对应的imei地址
39.银行预留手机号对应的idfa地址

 */
drop table if exists cid_bankCardMobile_derived_3m ;


create table cid_bankCardMobile_derived_3m as
select t1.event_cid,
       t1.event_bankCardMobile,
       size(collect_set(t2.event_certNo)) as bankCardMobile_dist_centNo_3m_am,
       size(collect_set(t2.event_cid)) as bankCardMobile_dist_cid_3m_am,
       size(collect_set(t2.event_bankCardNo)) as bankCardMobile_dist_bankCardNo_3m_am,
       size(collect_set(t2.event_deviceId)) as bankCardMobile_dist_deviceId_3m_am,
       size(collect_set(t2.event_imei)) as bankCardMobile_dist_imei_3m_am,
       size(collect_set(t2.event_idfa)) as bankCardMobile_dist_idfa_3m_am
from
    (select event_cid,
            event_bankCardMobile,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select distinct event_bankCardMobile,
                     collector_tstamp,
                     event_certNo,
                     event_cid ,
                     event_bankCardNo,
                     event_deviceId,
                     event_imei,
                     event_idfa
     from behavior_data_source_useful_flatten_2
     where event_bankCardMobile is not null
         and length(event_bankCardMobile) > 0
         and collector_tstamp is not null
         and length(collector_tstamp) > 0) t2 on t1.event_bankCardMobile = t2.event_bankCardMobile
where (t1.collector_tstamp >= t2.collector_tstamp)
    and (date_sub(t1.collector_tstamp,90) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid,
         t1.event_bankCardMobile ;

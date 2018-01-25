/* 1.客户号短期多次获额 */
use ${hiveconf:source_database};


drop table if exists cid_apply_derived_1m_hf;


create table cid_apply_derived_1m_hf as
select t1.event_cid,
       count(t1.event_cid) as cid_apply_1m_hf
from
    (select event_cid,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select event_cid,
            collector_tstamp
     from behavior_data_source_useful_flatten_2
     where collector_tstamp is not null
         and length(collector_tstamp) > 0
         and event_name = "app_apply") t2 on t1.event_cid = t2.event_cid
where (t1.collector_tstamp >= t2.collector_tstamp)
    and (date_sub(t1.collector_tstamp,INTERVAL 30 DAY) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid;

/* 3.客户号短期多张绑卡 */
drop table if exists cid_bind_bankcardno_derived_1m_hf;


create table cid_bind_bankcardno_derived_1m_hf as
select t1.event_cid,
       count(t2.event_bankcardno) as cid_bind_bankcardno_1m_hf
from
    (select event_cid,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select event_cid,
            event_bankcardno,
            collector_tstamp
     from behavior_data_source_useful_flatten_2
     where event_bankcardno is not null
         and length(event_bankcardno) >0
         and collector_tstamp is not null
         and length(collector_tstamp) > 0
         and event_name = "app_bindcard") t2 on t1.event_cid = t2.event_cid
where (t1.collector_tstamp >= t2.collector_tstamp)
    and (date_sub(t1.collector_tstamp,INTERVAL 30 DAY) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid;

/* 4.手机号短期多次获额 */
drop table if exists mobile_apply_derived_1m_hf;


create table mobile_apply_derived_1m_hf as
select t3.event_cid,
       t3.event_mobile,
       t4.mobile_apply_1m_hf
from
    (select event_cid,
            event_mobile,
            collector_tstamp
     from cid_first_loan) t3
left outer join
    (select t1.event_mobile,
            t1.collector_tstamp,
            count(t2.event_mobile) as mobile_apply_1m_hf
     from
         (select event_cid,
                 event_mobile,
                 collector_tstamp
          from cid_first_loan) t1
     left outer join
         (select event_cid,
                 event_mobile,
                 collector_tstamp
          from behavior_data_source_useful_flatten_2
          where event_mobile is not null
              and length(event_mobile) >0
              and collector_tstamp is not null
              and length(collector_tstamp) > 0
              and event_name = "app_apply") t2 on t1.event_mobile = t2.event_mobile
     where (t1.collector_tstamp >= t2.collector_tstamp)
         and (date_sub(t1.collector_tstamp,INTERVAL 30 DAY) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_mobile,
              t1.collector_tstamp) t4 on t3.event_mobile = t4.event_mobile
and t3.collector_tstamp = t4.collector_tstamp;

/* 6.手机号短期多张绑卡 */
drop table if exists mobile_bind_bankcardno_derived_1m_hf;


create table mobile_bind_bankcardno_derived_1m_hf as
select t3.event_cid,
       t3.event_mobile,
       t4.mobile_bind_bankcardno_1m_hf
from
    (select event_cid,
            event_mobile,
            collector_tstamp
     from cid_first_loan) t3
left outer join
    (select t1.event_mobile,
            t1.collector_tstamp,
            count(t2.event_mobile) as mobile_bind_bankcardno_1m_hf
     from
         (select event_cid,
                 event_mobile,
                 collector_tstamp
          from cid_first_loan) t1
     left outer join
         (select event_cid,
                 event_mobile,
                 collector_tstamp
          from behavior_data_source_useful_flatten_2
          where event_mobile is not null
              and length(event_mobile) >0
              and collector_tstamp is not null
              and length(collector_tstamp) > 0
              and event_name = "app_bindcard") t2 on t1.event_mobile = t2.event_mobile
     where (t1.collector_tstamp >= t2.collector_tstamp)
         and (date_sub(t1.collector_tstamp,INTERVAL 30 DAY) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_mobile,
              t1.collector_tstamp) t4 on t3.event_mobile = t4.event_mobile
and t3.collector_tstamp = t4.collector_tstamp;

/* 7.身份证号短期多次绑定账户 */
drop table if exists certno_bind_cid_derived_1m_hf;


create table certno_bind_cid_derived_1m_hf as
select t3.event_cid,
       t3.event_certno,
       t4.certno_bind_cid_1m_hf
from
    (select event_cid,
            event_certno,
            collector_tstamp
     from cid_first_loan) t3
left outer join
    (select t1.event_certno,
            t1.collector_tstamp,
            count(t1.event_certno) as certno_bind_cid_1m_hf
     from
         (select event_cid,
                 event_certno,
                 collector_tstamp
          from cid_first_loan) t1
     left outer join
         (select event_cid,
                 event_certno,
                 collector_tstamp
          from behavior_data_source_useful_flatten_2
          where event_certno is not null
              and length(event_certno) >0
              and collector_tstamp is not null
              and length(collector_tstamp) > 0
              and event_name = "app_bindidcard") t2 on t1.event_certno = t2.event_certno
     where (t1.collector_tstamp >= t2.collector_tstamp)
         and (date_sub(t1.collector_tstamp,INTERVAL 30 DAY) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_certno,
              t1.collector_tstamp) t4 on t3.event_certno = t4.event_certno
and t3.collector_tstamp = t4.collector_tstamp;

/* 8.银行卡号短期多次绑定账户 */
drop table if exists bankcardno_bind_cid_derived_1m_hf;


create table bankcardno_bind_cid_derived_1m_hf as
select t3.event_cid,
       t3.event_bankcardno,
       t4.bankcardno_bind_cid_1m_hf
from
    (select event_cid,
            event_bankcardno,
            collector_tstamp
     from cid_first_loan) t3
left outer join
    (select t1.event_bankcardno,
            t1.collector_tstamp,
            count(t1.event_bankcardno) as bankcardno_bind_cid_1m_hf
     from
         (select event_cid,
                 event_bankcardno,
                 collector_tstamp
          from cid_first_loan) t1
     left outer join
         (select event_cid,
                 event_bankcardno,
                 collector_tstamp
          from behavior_data_source_useful_flatten_2
          where event_bankcardno is not null
              and length(event_bankcardno) >0
              and collector_tstamp is not null
              and length(collector_tstamp) > 0
              and event_name = "app_bindcard") t2 on t1.event_bankcardno = t2.event_bankcardno
     where (t1.collector_tstamp >= t2.collector_tstamp)
         and (date_sub(t1.collector_tstamp,INTERVAL 30 DAY) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_bankcardno,
              t1.collector_tstamp) t4 on t3.event_bankcardno = t4.event_bankcardno
and t3.collector_tstamp = t4.collector_tstamp;



alter table cid_apply_derived_1m_hf add index index_event_cid (event_cid);
alter table cid_bind_bankcardno_derived_1m_hf add index index_event_cid (event_cid);
alter table mobile_apply_derived_1m_hf add index index_event_cid (event_cid);
alter table mobile_bind_bankcardno_derived_1m_hf add index index_event_cid (event_cid);
alter table certno_bind_cid_derived_1m_hf add index index_event_cid (event_cid);
alter table bankcardno_bind_cid_derived_1m_hf add index index_event_cid (event_cid);


drop table if exists cid_derived_hs_1m;


create table cid_derived_hs_1m as
select t0.event_cid,
       t1.cid_apply_1m_hf,
       t2.cid_bind_bankcardno_1m_hf,
       t3.mobile_apply_1m_hf,
       t4.mobile_bind_bankcardno_1m_hf,
       t5.certno_bind_cid_1m_hf,
       t6.bankcardno_bind_cid_1m_hf
from
    (select event_cid
     from cid_first_loan) t0
left outer join cid_apply_derived_1m_hf as t1 on t0.event_cid = t1.event_cid
left outer join cid_bind_bankcardno_derived_1m_hf as t2 on t0.event_cid = t2.event_cid
left outer join mobile_apply_derived_1m_hf as t3 on t0.event_cid = t3.event_cid
left outer join mobile_bind_bankcardno_derived_1m_hf as t4 on t0.event_cid = t4.event_cid
left outer join certno_bind_cid_derived_1m_hf as t5 on t0.event_cid = t5.event_cid
left outer join bankcardno_bind_cid_derived_1m_hf as t6 on t0.event_cid = t6.event_cid;

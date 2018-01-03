/* 3.客户号短期多张绑卡 */
drop table if exists cid_bind_bankcardno_derived_3m_hf;


create table cid_bind_bankcardno_derived_3m_hf as
select t1.event_cid,
       count(t2.event_bankcardno) as cid_bind_bankcardno_3m_hf
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
    and (date_sub(t1.collector_tstamp,90) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid;

/* 6.手机号短期多张绑卡 */
drop table if exists mobile_bind_bankcardno_derived_3m_hf;


create table mobile_bind_bankcardno_derived_3m_hf as
select t3.event_cid,
       t3.event_mobile,
       t4.mobile_bind_bankcardno_3m_hf
from
    (select event_cid,
            event_mobile,
            collector_tstamp
     from cid_first_loan) t3
left outer join
    (select t1.event_mobile,
            t1.collector_tstamp,
            count(t2.event_mobile) as mobile_bind_bankcardno_3m_hf
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
         and (date_sub(t1.collector_tstamp,90) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_mobile,
              t1.collector_tstamp) t4 on t3.event_mobile = t4.event_mobile
and t3.collector_tstamp = t4.collector_tstamp;

/* 7.身份证号短期多次绑定账户 */
drop table if exists certno_bind_cid_derived_3m_hf;


create table certno_bind_cid_derived_3m_hf as
select t3.event_cid,
       t3.event_certno,
       t4.certno_bind_cid_3m_hf
from
    (select event_cid,
            event_certno,
            collector_tstamp
     from cid_first_loan) t3
left outer join
    (select t1.event_certno,
            t1.collector_tstamp,
            count(t1.event_certno) as certno_bind_cid_3m_hf
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
         and (date_sub(t1.collector_tstamp,90) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_certno,
              t1.collector_tstamp) t4 on t3.event_certno = t4.event_certno
and t3.collector_tstamp = t4.collector_tstamp;

/* 8.银行卡号短期多次绑定账户 */
drop table if exists bankcardno_bind_cid_derived_3m_hf;


create table bankcardno_bind_cid_derived_3m_hf as
select t3.event_cid,
       t3.event_bankcardno,
       t4.bankcardno_bind_cid_3m_hf
from
    (select event_cid,
            event_bankcardno,
            collector_tstamp
     from cid_first_loan) t3
left outer join
    (select t1.event_bankcardno,
            t1.collector_tstamp,
            count(t1.event_bankcardno) as bankcardno_bind_cid_3m_hf
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
         and (date_sub(t1.collector_tstamp,90) <= substring(t2.collector_tstamp, 1,10))
     group by t1.event_bankcardno,
              t1.collector_tstamp) t4 on t3.event_bankcardno = t4.event_bankcardno
and t3.collector_tstamp = t4.collector_tstamp;


drop table if exists cid_derived_hs_3m;


create table cid_derived_hs_3m as
select t1.event_cid,
       t2.cid_bind_bankcardno_3m_hf,
       t3.mobile_bind_bankcardno_3m_hf,
       t4.certno_bind_cid_3m_hf,
       t5.bankcardno_bind_cid_3m_hf
from
    (select event_cid
     from cid_first_loan) t1
left outer join cid_bind_bankcardno_derived_3m_hf as t2 on t1.event_cid = t2.event_cid
left outer join mobile_bind_bankcardno_derived_3m_hf as t3 on t1.event_cid = t3.event_cid
left outer join certno_bind_cid_derived_3m_hf as t4 on t1.event_cid = t4.event_cid
left outer join bankcardno_bind_cid_derived_3m_hf as t5 on t1.event_cid = t5.event_cid;
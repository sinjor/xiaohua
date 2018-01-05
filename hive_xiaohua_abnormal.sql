/* 1.异地登录（近一天、一周、一个月、三个月，ip所属城市个数）*/ /* 1.异地登录（近三个月，ip所属城市个数）*/
drop table if exists ipcity_count_derived_3m_abn;


create table ipcity_count_derived_3m_abn as
select t1.event_cid,
       count(distinct t2.event_ipcity) as ipcity_count_3m_abn
from
    (select event_cid,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select event_cid,
            event_ipcity,
            collector_tstamp
     from behavior_data_source_useful_flatten_2
     where event_ipcity is not null
         and length(event_ipcity) >0
         and collector_tstamp is not null
         and length(collector_tstamp) > 0) t2 on t1.event_cid = t2.event_cid
where (t1.collector_tstamp >= t2.collector_tstamp)
    and (date_sub(t1.collector_tstamp,90) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid;

/* 1.异地登录（近一个月、ip所属城市个数）*/
drop table if exists ipcity_count_derived_1m_abn;


create table ipcity_count_derived_1m_abn as
select t1.event_cid,
       count(distinct t2.event_ipcity) as ipcity_count_1m_abn
from
    (select event_cid,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select event_cid,
            event_ipcity,
            collector_tstamp
     from behavior_data_source_useful_flatten_2
     where event_ipcity is not null
         and length(event_ipcity) >0
         and collector_tstamp is not null
         and length(collector_tstamp) > 0) t2 on t1.event_cid = t2.event_cid
where (t1.collector_tstamp >= t2.collector_tstamp)
    and (date_sub(t1.collector_tstamp,30) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid;

/* 1.异地登录（近7天、ip所属城市个数）*/
drop table if exists ipcity_count_derived_7d_abn;


create table ipcity_count_derived_7d_abn as
select t1.event_cid,
       count(distinct t2.event_ipcity) as ipcity_count_7d_abn
from
    (select event_cid,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select event_cid,
            event_ipcity,
            collector_tstamp
     from behavior_data_source_useful_flatten_2
     where event_ipcity is not null
         and length(event_ipcity) >0
         and collector_tstamp is not null
         and length(collector_tstamp) > 0) t2 on t1.event_cid = t2.event_cid
where (t1.collector_tstamp >= t2.collector_tstamp)
    and (date_sub(t1.collector_tstamp,7) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid;

/* 1.异地登录（近1天、ip所属城市个数） 重点关注，1天的精度可能会出问题*/
drop table if exists ipcity_count_derived_1d_abn;


create table ipcity_count_derived_1d_abn as
select t1.event_cid,
       count(distinct t2.event_ipcity) as ipcity_count_1d_abn
from
    (select event_cid,
            collector_tstamp
     from cid_first_loan) t1
left outer join
    (select event_cid,
            event_ipcity,
            collector_tstamp
     from behavior_data_source_useful_flatten_2
     where event_ipcity is not null
         and length(event_ipcity) >0
         and collector_tstamp is not null
         and length(collector_tstamp) > 0) t2 on t1.event_cid = t2.event_cid
where (t1.collector_tstamp >= t2.collector_tstamp)
    and (date_sub(t1.collector_tstamp,1) <= substring(t2.collector_tstamp, 1,10))
group by t1.event_cid;


drop table if exists cid_derived_abn_all;


create table cid_derived_abn_all as
select t1.event_cid,
       t2.ipcity_count_3m_abn,
       t3.ipcity_count_1m_abn,
       t4.ipcity_count_7d_abn,
       t5.ipcity_count_1d_abn
from
    (select event_cid
     from cid_first_loan) t1
left outer join ipcity_count_derived_3m_abn as t2 on t1.event_cid = t2.event_cid
left outer join ipcity_count_derived_1m_abn as t3 on t1.event_cid = t3.event_cid
left outer join ipcity_count_derived_7d_abn as t4 on t1.event_cid = t4.event_cid
left outer join ipcity_count_derived_1d_abn as t5 on t1.event_cid = t5.event_cid;
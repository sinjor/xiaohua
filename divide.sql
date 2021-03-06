--set train_table=aaaaaaaaaaaa;
--set train_database=bbbbbbbbbb;
--set test_table=tttttttt;
--set test_database=eeeeeeeeee;
--set original_database=default;
--set original_table=behavior_data_source_useful_flatten_2;

drop database if exists ${hiveconf:train_database} cascade;


create database ${hiveconf:train_database};


drop database if exists ${hiveconf:test_database} cascade;


create database ${hiveconf:test_database};

use ${hiveconf:train_database};

--create table ${hiveconf:original_table} as
--select *
--from default.${hiveconf:original_table};

drop table if exists ${hiveconf:train_table};


create table ${hiveconf:train_table} as
select t2.* from
    (select event_cid, min(collector_tstamp) as collector_tstamp
     from ${hiveconf:original_table}
     where event_name = "app_loan"
         and collector_tstamp is not null
         and length(collector_tstamp) > 0
     group by event_cid) t1
left outer join ${hiveconf:original_table} as t2 on t1.event_cid = t2.event_cid
where t1.collector_tstamp < "2017-10-01 00:00:00";

--create table ${hiveconf:train_table} as
--select t2.rand_id,
--       t3.*
--from
--    (select row_number() over(
--                              order by rand()) as rand_id,
--            event_cid
--     from
--         (select distinct event_cid
--          from ${hiveconf:original_table}) t1)t2
--left outer join ${hiveconf:original_table} as t3 on t2.event_cid = t3.event_cid
--where rand_id <70000;
 use ${hiveconf:test_database};

--create table ${hiveconf:original_table} as
--select *
--from default.${hiveconf:original_table};

drop table if exists ${hiveconf:test_table};


create table ${hiveconf:test_table} as
select t2.* from
    (select event_cid, min(collector_tstamp) as collector_tstamp
     from ${hiveconf:original_table}
     where event_name = "app_loan"
         and collector_tstamp is not null
         and length(collector_tstamp) > 0
     group by event_cid) t1
left outer join ${hiveconf:original_table} as t2 on t1.event_cid = t2.event_cid
where t1.collector_tstamp >= "2017-10-01 00:00:00";

--select t2.rand_id,
--       t3.*
--from
--    (select row_number() over(
--                              order by rand()) as rand_id,
--            event_cid
--     from
--         (select distinct event_cid
--          from ${hiveconf:original_table}) t1)t2
--left outer join ${hiveconf:original_table} as t3 on t2.event_cid = t3.event_cid
--where rand_id >=70000;
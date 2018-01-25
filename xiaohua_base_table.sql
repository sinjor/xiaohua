#1.创建原始表
create table behavior_data_source_useful_flatten_2
(
app_derived_mobile varchar(64),
app_derived_certno varchar(64),
app_derived_name varchar(64),
event_mobile varchar(64),
event_cid varchar(64),
event_cli_name varchar(64),
event_certno varchar(64),
event_residence varchar(64),
event_education varchar(64),
event_organization varchar(64),
event_companyphone varchar(64),
event_position varchar(64),
event_workyears varchar(64),
event_contactsname varchar(64),
event_contactsmobile varchar(64),
event_contactsrelationship varchar(64),
event_openid varchar(64),
event_wechatnickname varchar(64),
event_bankbin varchar(64),
event_bankcardno varchar(64),
event_bankcardtype varchar(64),
event_bankcardcode varchar(64),
event_bankcardname varchar(64),
event_bankcardmobile varchar(64),
event_srcchannel varchar(64),
event_platform varchar(64),
event_deviceid varchar(64),
event_smartid varchar(64),
event_fptokenid varchar(64),
event_fingerprint varchar(64),
event_useragent varchar(1024),
event_isemulator varchar(64),
event_idfa varchar(64),
event_mac varchar(64),
event_imei varchar(64),
event_ip varchar(64),
event_longitude varchar(64),
event_latitude varchar(64),
event_gpscity varchar(64),
event_mobilecity varchar(64),
event_trueip varchar(64),
event_ipprovince varchar(64),
event_ipcity varchar(64),
event_iplongitude varchar(64),
event_iplatitude varchar(64),
event_devicebssid varchar(64),
event_loanamount varchar(64),
event_loanterm varchar(64),
event_loandate varchar(64),
event_loanresult varchar(64),
event_producttype varchar(64),
app_id varchar(64),
collector_tstamp varchar(64),
event_name varchar(64),
network_userid varchar(64),
user_ipaddress varchar(64),
collector_date varchar(64),
dvce_created_tstamp varchar(64),
etl_tstamp varchar(64),
derived_tstamp varchar(64),
collector_time varchar(64),
antifraud_dataenrich_version varchar(64),
event_id varchar(64)
)
#linux下导出的，没有换行符\t
load data infile "G:/taodata/project/xiaohua_finance/behavior_data_source_useful_flatten_2/behavior_data_source_useful_flatten_2.csv"
into table behavior_data_source_useful_flatten_2 
fields terminated by '\t' optionally enclosed by '"' escaped by '"'
lines terminated by '\n';
ignore 1 lines;

#给源表添加索引
alter table behavior_data_source_useful_flatten_2 add index index_event_cid (event_cid);
alter table behavior_data_source_useful_flatten_2 add index index_collector_tstamp (collector_tstamp);
alter table behavior_data_source_useful_flatten_2 add index index_event_useragent (event_useragent);
alter table behavior_data_source_useful_flatten_2 add index index_event_name (event_name);
alter table behavior_data_source_useful_flatten_2 add index index_event_bankCardMobile (event_bankCardMobile);
alter table behavior_data_source_useful_flatten_2 add index index_event_bankCardNo (event_bankCardNo);
alter table behavior_data_source_useful_flatten_2 add index index_event_mobile (event_mobile);
alter table behavior_data_source_useful_flatten_2 add index index_event_certNo (event_certNo);
alter table behavior_data_source_useful_flatten_2 add index index_event_cid_time (event_cid, collector_tstamp);
alter table behavior_data_source_useful_flatten_2 add index index_event_trueip (event_trueip);

#2创建label表
create table ml_lables(
cid varchar(64),
label varchar(8)
);
#分隔符是 ,且换行符是\r\n，没有写\r 则\r会作为数据内容存在表格中
load data infile "G:/taodata/project/xiaohua_finance/ml_labels.csv"
into table ml_lables 
fields terminated by ',' optionally enclosed by '"' escaped by '"'
lines terminated by '\r\n'
ignore 1 lines;
#给label表添加索引
alter table ml_lables add unique (cid);


#3生成 cid_first_loan
drop table if exists cid_first_loan;

create table cid_first_loan as
select 
            t2.event_mobile,
            t2.event_cid,
            t2.event_cli_name,
            t2.event_certNo,
            t2.event_residence,
            t2.event_education,
            t2.event_organization,
            t2.event_companyPhone,
            t2.event_position,
            t2.event_workYears,
            t2.event_contactsName,
            t2.event_contactsMobile,
            t2.event_contactsRelationship,
            t2.event_openid,
            t2.event_wechatNickname,
            t2.event_bankBin,
            t2.event_bankCardNo,
            t2.event_bankCardType,
            t2.event_bankCardCode,
            t2.event_bankCardName,
            t2.event_bankCardMobile,
            t2.event_srcChannel,
            t2.event_platform,
            t2.event_deviceId,
            t2.event_smartId,
            t2.event_fpTokenID,
            t2.event_fingerprint,
            t2.event_userAgent,
            t2.event_isEmulator,
            t2.event_idfa,
            t2.event_mac,
            t2.event_imei,
            t2.event_ip,
            t2.event_longitude,
            t2.event_latitude,
            t2.event_gpsCity,
            t2.event_mobileCity,
            t2.event_trueIP,
            t2.event_ipProvince,
            t2.event_ipCity,
            t2.event_ipLongitude,
            t2.event_ipLatitude,
            t2.event_deviceBssid,
            t2.event_loanAmount,
            t2.event_loanTerm,
            t2.event_loanDate,
            t2.event_loanResult,
            t2.event_productType,
            t2.event_name,
            t2.event_id,
            t2.collector_tstamp
from
(select event_cid, min(collector_tstamp) as collector_tstamp_first_loan
from behavior_data_source_useful_flatten_2
where collector_tstamp is not null
and length(collector_tstamp) > 0
and event_name = "app_loan"
and event_cid is not null
and length(event_cid)>0
group by event_cid ) t1 
left outer join 
(select * 
from behavior_data_source_useful_flatten_2 where collector_tstamp is not null
and length(collector_tstamp) > 0
and event_name = "app_loan"
and event_cid is not null
and length(event_cid)>0) t2 on t1.collector_tstamp_first_loan = t2.collector_tstamp and t1.event_cid = t2.event_cid;

#给cid_first_loan表添加索引
alter table cid_first_loan add unique (event_cid);
alter table cid_first_loan add index index_collector_tstamp (collector_tstamp);
alter table cid_first_loan add index index_event_bankCardMobile (event_bankCardMobile);
alter table cid_first_loan add index index_event_bankCardNo (event_bankCardNo);
alter table cid_first_loan add index index_event_mobile (event_mobile);
alter table cid_first_loan add index index_event_certNo (event_certNo);
alter table cid_first_loan add index index_event_bankCardName (event_bankCardName);
alter table cid_first_loan add index index_event_ipCity (event_ipCity);
alter table cid_first_loan add index index_event_mobileCity (event_mobileCity);
alter table cid_first_loan add index index_event_mobileCity (event_ipProvince);
alter table cid_first_loan add index index_event_trueip (event_trueip);


#4生成cid_first_apply表 用于xiaohua_other.sql
drop table if exists cid_first_apply;

create table cid_first_apply as
select distinct t2.*
from
    (select event_cid,
            min(collector_tstamp) as collector_tstamp_first_apply

     from behavior_data_source_useful_flatten_2
     where event_name='app_apply'
         and event_cid is not null
         and length(event_cid) > 0
         and collector_tstamp is not  null
         and length(collector_tstamp) > 0
     group by event_cid) t1
left outer join
    (select event_cid,
            event_residence,
            event_education,
            event_organization,
            event_companyPhone,
            event_position,
            event_workYears,
            event_contactsName,
            event_contactsMobile,
            event_contactsRelationship,
            collector_tstamp,
            event_name,
            event_id
     from behavior_data_source_useful_flatten_2
     where event_name='app_apply'
         and event_cid is not null
         and length(event_cid) > 0
         and collector_tstamp is not  null
         and length(collector_tstamp) > 0) t2 on t1.event_cid = t2.event_cid
and t1.collector_tstamp_first_apply = t2.collector_tstamp;

#存在重复event_cid临时表
drop table if exists tmp_tb1;
create table tmp_tb1 as
select event_cid
from cid_first_apply
group by event_cid
having count(event_cid) > 1;

#存在最小值event_id的临时表
drop table if exists tmp_tb2;
create table tmp_tb2 as
select min(event_id) as event_id
from cid_first_apply
group by event_cid
having count(event_cid)>1;
#给cid_first_apply tmp_tb1 tmp_tb2 建立索引
alter table cid_first_apply add index index_cid (event_cid);
alter table cid_first_apply add index index_id (event_id);
alter table tmp_tb1 add index index_cid (event_cid);
alter table tmp_tb2 add index index_id (event_id);
#对cid_first_apply根据event_id去重
delete
from cid_first_apply
where event_cid in (select event_cid from tmp_tb1)
    and event_id not in (select event_id from  tmp_tb2);



alter table cid_first_apply drop index index_cid ;
#添加索引
alter table cid_first_apply add unique (event_cid);
alter table cid_first_apply add index index_event_education (event_education);
alter table cid_first_apply add index index_event_position (event_position);
alter table cid_first_apply add index index_event_workYears (event_workYears);
alter table cid_first_apply add index index_event_contactsRelationship (event_contactsRelationship);


#生成表cid_last_apply_before_loan 借款前最近一次获额 用于new_feature_extract
drop table if exists cid_last_apply_before_loan;


create table cid_last_apply_before_loan as
select t3.loan_collector_tstamp,t4.*
from
    (select t1.collector_tstamp as loan_collector_tstamp,
            max(t2.collector_tstamp) as collector_tstamp_last_apply,
            t2.event_cid
     from
         (select event_cid,
                 collector_tstamp
          from cid_first_loan) t1
     inner join
         (select event_cid,
                 collector_tstamp
          from behavior_data_source_useful_flatten_2
          where event_name='app_apply'
              and event_cid is not null
              and length(event_cid) > 0
              and collector_tstamp is not  null
              and length(collector_tstamp) > 0 ) as t2 on t1.event_cid = t2.event_cid
     where t1.collector_tstamp >= t2.collector_tstamp
     group by t2.event_cid) t3
left outer join
    (select event_cid,
            event_residence,
            event_education,
            event_organization,
            event_companyPhone,
            event_position,
            event_workYears,
            event_contactsName,
            event_contactsMobile,
            event_contactsRelationship,
            event_ipCity,
            event_mobilecity,
            event_gpsCity,
            event_longitude,
            event_latitude,
            event_iplatitude,
            event_ipLongitude,
            collector_tstamp,
            event_deviceid,
            event_idfa,
            event_imei,
            event_id
     from behavior_data_source_useful_flatten_2
     where event_name='app_apply'
         and event_cid is not null
         and length(event_cid) > 0
         and collector_tstamp is not  null
         and length(collector_tstamp) > 0) t4 on t3.event_cid = t4.event_cid and t3.collector_tstamp_last_apply = t4.collector_tstamp;




drop table if exists tmp_tb1;
create table tmp_tb1 as
select event_cid
from cid_last_apply_before_loan
group by event_cid
having count(event_cid) > 1;

drop table if exists tmp_tb2;
create table tmp_tb2 as
select min(event_id) as event_id
from cid_last_apply_before_loan
group by event_cid
having count(event_cid)>1;

alter table cid_last_apply_before_loan add index index_cid (event_cid);
alter table cid_last_apply_before_loan add index index_id (event_id);
alter table tmp_tb1 add index index_cid (event_cid);
alter table tmp_tb2 add index index_id (event_id);

delete
from cid_last_apply_before_loan
where event_cid in (select event_cid from tmp_tb1)
    and event_id not in (select event_id from  tmp_tb2);

alter table cid_last_apply_before_loan drop index index_id ;



alter table cid_last_apply_before_loan add unique (event_cid);


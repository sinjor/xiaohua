-- education_code
-- position_code
-- workYears_code
-- contactsRelationship_code
-- bankCardName_code
-- ipCity_code
-- mobileCity_code
-- ipProvince_code
 use ${hiveconf:source_database};


drop table if exists education_code;


set @row_number = 0;


create table education_code as
select t.event_education,
       case
           when t.event_education is null
                or length(event_education) = 0 then 0
           else @row_number := @row_number + 1
       end as event_education_code
from
    (select distinct event_education as event_education
     from behavior_data_source_useful_flatten_2) t
order by t.event_education;


drop table if exists position_code;


set @row_number = 0;


create table position_code as
select t.event_position,
       case
           when t.event_position is null
                or length(event_position) = 0 then 0
           else @row_number:=@row_number + 1
       end as event_position_code
from
    (select distinct event_position as event_position
     from behavior_data_source_useful_flatten_2) t
order by t.event_position;


drop table if exists workYears_code;


set @row_number = 0;


create table workYears_code as
select t.event_workyears,
       case
           when t.event_workyears is null
                or length(event_workyears) = 0 then 0
           else @row_number:=@row_number + 1
       end as event_workyears_code
from
    (select distinct event_workyears as event_workyears
     from behavior_data_source_useful_flatten_2) t
order by t.event_workyears;


drop table if exists contactsRelationship_code;


set @row_number = 0;


create table contactsRelationship_code as
select t.event_contactsrelationship,
       case
           when t.event_contactsrelationship is null
                or length(event_contactsrelationship) = 0 then 0
           else @row_number:=@row_number + 1
       end as event_contactsrelationship_code
from
    (select distinct event_contactsrelationship as event_contactsrelationship
     from behavior_data_source_useful_flatten_2) t
order by t.event_contactsrelationship;


drop table if exists bankCardName_code;


set @row_number = 0;


create table bankCardName_code as
select t.event_bankcardname,
       case
           when t.event_bankcardname is null
                or length(event_bankcardname) = 0 then 0
           else @row_number:=@row_number + 1
       end as event_bankcardname_code
from
    (select distinct event_bankcardname as event_bankcardname
     from behavior_data_source_useful_flatten_2) t
order by t.event_bankcardname;


drop table if exists ipCity_code;


set @row_number = 0;


create table ipCity_code as
select t.event_ipcity,
       case
           when t.event_ipcity is null
                or length(event_ipcity) = 0 then 0
           else @row_number:=@row_number + 1
       end as event_ipcity_code
from
    (select distinct event_ipcity as event_ipcity
     from behavior_data_source_useful_flatten_2) t
order by t.event_ipcity;


drop table if exists mobileCity_code;


set @row_number = 0;


create table mobileCity_code as
select t.event_mobilecity,
       case
           when t.event_mobilecity is null
                or length(event_mobilecity) = 0 then 0
           else @row_number:=@row_number + 1
       end as event_mobilecity_code
from
    (select distinct event_mobilecity as event_mobilecity
     from behavior_data_source_useful_flatten_2) t
order by t.event_mobilecity;


drop table if exists ipProvince_code;


set @row_number = 0;


create table ipProvince_code as
select t.event_ipprovince,
       case
           when t.event_ipprovince is null
                or length(event_ipprovince) = 0 then 0
           else @row_number:=@row_number + 1
       end as event_ipprovince_code
from
    (select distinct event_ipprovince as event_ipprovince
     from behavior_data_source_useful_flatten_2) t
order by t.event_ipprovince;


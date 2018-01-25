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
       (@row_number:=@row_number + 1) as event_education_code
from
    (select distinct event_education as event_education
     from behavior_data_source_useful_flatten_2
     where event_education is not null) t
    order by t.event_education;


drop table if exists position_code;
set @row_number = 0;

create table position_code as
select t.event_position,
       (@row_number:=@row_number + 1) as event_position_code
from
    (select distinct event_position as event_position
     from behavior_data_source_useful_flatten_2
     where event_position is not null) t
    order by t.event_position;


drop table if exists workYears_code;
set @row_number = 0;

create table workYears_code as
select t.event_workyears,
       (@row_number:=@row_number + 1) as event_workyears_code
from
    (select distinct event_workyears as event_workyears
     from behavior_data_source_useful_flatten_2
     where event_workyears is not null) t
    order by t.event_workyears;


drop table if exists contactsRelationship_code;
set @row_number = 0;

create table contactsRelationship_code as
select t.event_contactsrelationship,
       (@row_number:=@row_number + 1) as event_contactsrelationship_code
from
    (select distinct event_contactsrelationship as event_contactsrelationship
     from behavior_data_source_useful_flatten_2
     where event_contactsrelationship is not null) t
    order by t.event_contactsrelationship;


drop table if exists bankCardName_code;
set @row_number = 0;

create table bankCardName_code as
select t.event_bankcardname,
       (@row_number:=@row_number + 1) as event_bankcardname_code
from
    (select distinct event_bankcardname as event_bankcardname
     from behavior_data_source_useful_flatten_2
     where event_bankcardname is not null) t
    order by t.event_bankcardname;


drop table if exists ipCity_code;
set @row_number = 0;

create table ipCity_code as
select t.event_ipcity,
       (@row_number:=@row_number + 1) as event_ipcity_code
from
    (select distinct event_ipcity as event_ipcity
     from behavior_data_source_useful_flatten_2
     where event_ipcity is not null) t
    order by t.event_ipcity;


drop table if exists mobileCity_code;
set @row_number = 0;

create table mobileCity_code as
select t.event_mobilecity,
       (@row_number:=@row_number + 1) as event_mobilecity_code
from
    (select distinct event_mobilecity as event_mobilecity
     from behavior_data_source_useful_flatten_2
     where event_mobilecity is not null) t
order by t.event_mobilecity;

drop table if exists ipProvince_code;
set @row_number = 0;

create table ipProvince_code as
select t.event_ipprovince,
       (@row_number:=@row_number + 1) as event_ipprovince_code
from
    (select distinct event_ipprovince as event_ipprovince
     from behavior_data_source_useful_flatten_2
     where event_ipprovince is not null) t
    order by t.event_ipprovince;
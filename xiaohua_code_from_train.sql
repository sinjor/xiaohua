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


create table education_code as
select t.event_education,
       row_number() over(
                         order by t.event_education) as event_education_code
from
    (select distinct event_education as event_education
     from ${hiveconf:source_table}
     where event_education is not null) t;


drop table if exists position_code;


create table position_code as
select t.event_position,
       row_number() over(
                         order by t.event_position) as event_position_code
from
    (select distinct event_position as event_position
     from ${hiveconf:source_table}
     where event_position is not null) t;


drop table if exists workYears_code;


create table workYears_code as
select t.event_workyears,
       row_number() over(
                         order by t.event_workyears) as event_workyears_code
from
    (select distinct event_workyears as event_workyears
     from ${hiveconf:source_table}
     where event_workyears is not null) t;


drop table if exists contactsRelationship_code;


create table contactsRelationship_code as
select t.event_contactsrelationship,
       row_number() over(
                         order by t.event_contactsrelationship) as event_contactsrelationship_code
from
    (select distinct event_contactsrelationship as event_contactsrelationship
     from ${hiveconf:source_table}
     where event_contactsrelationship is not null) t;


drop table if exists bankCardName_code;


create table bankCardName_code as
select t.event_bankCardName,
       row_number() over(
                         order by t.event_bankcardname) as event_bankcardname_code
from
    (select distinct event_bankcardname as event_bankcardname
     from ${hiveconf:source_table}
     where event_bankcardname is not null) t;


drop table if exists ipCity_code;


create table ipCity_code as
select t.event_ipCity,
       row_number() over(
                         order by t.event_ipcity) as event_ipcity_code
from
    (select distinct event_ipcity as event_ipcity
     from ${hiveconf:source_table}
     where event_ipcity is not null) t;


drop table if exists mobileCity_code;


create table mobileCity_code as
select t.event_mobileCity,
       row_number() over(
                         order by t.event_mobilecity) as event_mobilecity_code
from
    (select distinct event_mobilecity as event_mobilecity
     from ${hiveconf:source_table}
     where event_mobilecity is not null) t;


drop table if exists ipProvince_code;


create table ipProvince_code as
select t.event_ipprovince,
       row_number() over(
                         order by t.event_ipprovince) as event_ipprovince_code
from
    (select distinct event_ipprovince as event_ipprovince
     from ${hiveconf:source_table}
     where event_ipprovince is not null) t;
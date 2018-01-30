#对转换表加索引
alter table education_code add index index_event_education (event_education);
alter table position_code add index index_event_position (event_position);
alter table workYears_code add index index_event_workYears (event_workYears);
alter table contactsRelationship_code add index index_event_contactsRelationship (event_contactsRelationship);
alter table bankCardName_code add index index_event_bankCardName (event_bankCardName);
alter table ipCity_code add index index_event_ipCity (event_ipCity);
alter table mobileCity_code add index index_event_mobileCity (event_mobileCity);
alter table ipProvince_code add index index_event_event_ipProvince (event_ipProvince);


drop table if exists cid_first_loan_source_features;
create table cid_first_loan_source_features as
select t1.event_cid,
       case
           when t1.event_certNo is not null
                and length(t1.event_certNo) > 0 then 1
           else 0
       end as event_certNo_flag,
       case
           when t2.event_residence is not null
                and length(t2.event_residence) > 0 then 1
           else 0
       end as event_residence_flag,
       ifnull(t3.event_education_code, 0) as event_education_code,
       case
           when t2.event_organization is not null
                and length(t2.event_organization) > 0 then 1
           else 0
       end as event_organization_flag,
       case
           when t2.event_companyPhone is not null
                and length(t2.event_companyPhone) > 0 then 1
           else 0
       end as event_companyPhone_flag,
       ifnull(t4.event_position_code, 0) as event_position_code,
       ifnull(t5.event_workYears_code, 0) as event_workYears_code,
       case
           when t2.event_contactsName is not null
                and length(t2.event_contactsName) > 0 then 1
           else 0
       end as event_contactsName_flag,
       case
           when t2.event_contactsMobile is not null
                and length(t2.event_contactsMobile) > 0 then 1
           else 0
       end as event_contactsMobile_flag,
       ifnull(t6.event_contactsRelationship_code, 0) as event_contactsrelationship_code,
       t1.event_bankCardType,
       t1.event_bankCardCode,
       t7.event_bankCardName_code,
       t1.event_srcChannel,
       t1.event_fpTokenID,
       t1.event_fingerprint,
       t8.event_ipCity_code,
       t9.event_mobileCity_code,
       t10.event_ipProvince_code,
       case
           when t1.event_longitude is not null
                and length(t1.event_longitude) > 0 then 1
           else 0
       end as event_longitude_flag,
       case
           when t1.event_latitude is not null
                and length(t1.event_latitude) > 0 then 1
           else 0
       end as event_latitude_flag,
       case
           when t1.event_ipLongitude is not null
                and length(t1.event_ipLongitude) > 0 then 1
           else 0
       end as event_ipLongitude_flag,
       case
           when t1.event_ipLatitude is not null
                and length(t1.event_ipLatitude) > 0 then 1
           else 0
       end as event_ipLatitude_flag,
       case
           when t1.event_longitude is not null
                and length(t1.event_longitude) > 0
                and t1.event_ipLongitude is not null
                and length(t1.event_ipLongitude) > 0
                and floor(cast(t1.event_longitude as DECIMAL) * 1000.0) = floor(cast(t1.event_ipLongitude as DECIMAL) * 1000.0)
                and t1.event_latitude is not null
                and length(t1.event_latitude) > 0
                and t1.event_ipLatitude is not null
                and length(t1.event_ipLatitude) > 0
                and floor(cast(t1.event_latitude as DECIMAL) * 1000.0) = floor(cast(t1.event_ipLatitude as DECIMAL) * 1000.0) then 1
           else 0
       end event_gps_ip_longitude_latitude_flag,
       case
           when length(t1.event_loanAmount) > 0 then floor(cast(t1.event_loanAmount as DECIMAL))
           else null
       end as event_loanAmount,
       case
           when length(t1.event_loanTerm) > 0 then cast(t1.event_loanTerm as signed)
           else null
       end as event_loanTerm,
       case
           when length(t1.event_producttype) > 0 then cast(t1.event_producttype as signed)
           else null
       end as event_producttype
from cid_first_loan t1
left outer join cid_first_apply t2 on t1.event_cid = t2.event_cid
left outer join education_code t3 on t2.event_education = t3.event_education
left outer join position_code t4 on t2.event_position = t4.event_position
left outer join workYears_code t5 on t2.event_workYears = t5.event_workYears
left outer join contactsRelationship_code t6 on t2.event_contactsRelationship = t6.event_contactsRelationship
left outer join bankCardName_code t7 on t7.event_bankCardName = t1.event_bankCardName
left outer join ipCity_code t8 on t1.event_ipCity = t8.event_ipCity
left outer join mobileCity_code t9 on t1.event_mobileCity = t9.event_mobileCity
left outer join ipProvince_code t10 on t1.event_ipProvince = t10.event_ipProvince ;



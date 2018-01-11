use ${hiveconf:source_database};


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
       t3.event_education_code,
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
       t4.event_position_code,
       t5.event_workYears_code,
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
       t6.event_contactsRelationship_code,
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
                and floor(cast(t1.event_longitude as float) * 1000.0) = floor(cast(t1.event_ipLongitude as float) * 1000.0)
                and t1.event_latitude is not null
                and length(t1.event_latitude) > 0
                and t1.event_ipLatitude is not null
                and length(t1.event_ipLatitude) > 0
                and floor(cast(t1.event_latitude as float) * 1000.0) = floor(cast(t1.event_ipLatitude as float) * 1000.0) then 1
           else 0
       end event_gps_ip_longitude_latitude_flag,
       cast(t1.event_loanAmount as int) as event_loanAmount,
       cast(t1.event_loanTerm as int) as event_loanTerm,
       cast(t1.event_productType as int) as event_productType

from cid_first_loan t1
left outer join
    (select *
     from
         (select row_number() over(partition by event_cid
                                   order by collector_tstamp asc) as cid_apply_order,
                 event_cid,
                 event_residence,
                 event_education,
                 event_organization,
                 event_companyPhone,
                 event_position,
                 event_workYears,
                 event_contactsName,
                 event_contactsMobile,
                 event_contactsRelationship
          from ${hiveconf:source_table}
          where event_name='app_apply'
              and event_cid is not null
              and length(event_cid) > 0
              and collector_tstamp is not  null
              and length(collector_tstamp) > 0 ) t
     where cid_apply_order=1 ) t2 on t1.event_cid = t2.event_cid
left outer join education_code t3 on t2.event_education = t3.event_education
left outer join position_code t4 on t2.event_position = t4.event_position
left outer join workYears_code t5 on t2.event_workYears = t5.event_workYears
left outer join contactsRelationship_code t6 on t2.event_contactsRelationship = t6.event_contactsRelationship
left outer join bankCardName_code t7 on t7.event_bankCardName = t1.event_bankCardName
left outer join ipCity_code t8 on t1.event_ipCity = t8.event_ipCity
left outer join mobileCity_code t9 on t1.event_mobileCity = t9.event_mobileCity
left outer join ipProvince_code t10 on t1.event_ipProvince = t10.event_ipProvince ;


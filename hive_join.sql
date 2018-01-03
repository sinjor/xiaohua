drop table if exists cid_derived_all ;

create table cid_derived_all as
select t1.event_cid,
       t2.certno_dist_cid_1m_am,
       t2.certno_dist_mobile_1m_am,
       t2.certno_dist_bankCardNo_1m_am,
       t2.certno_dist_bankCardMobile_1m_am,
       t2.certno_dist_deviceId_1m_am,
       t2.certno_dist_imei_1m_am,
       t2.certno_dist_idfa_1m_am,
       t3.cid_dist_bankCardNo_1m_am,
       t3.cid_dist_bankCardMobile_1m_am,
       t3.cid_dist_deviceId_1m_am,
       t3.cid_dist_idfa_1m_am,
       t3.cid_dist_imei_1m_am,
       t4.mobile_dist_cid_1m_am,
       t4.mobile_dist_certNo_1m_am,
       t4.mobile_dist_bankCardNo_1m_am,
       t4.mobile_dist_deviceId_1m_am,
       t4.mobile_dist_idfa_1m_am,
       t4.mobile_dist_imei_1m_am,
       t5.bankCardNo_dist_cid_1m_am,
       t5.bankCardNo_dist_mobile_1m_am,
       t5.bankCardNo_dist_deviceId_1m_am,
       t5.bankCardNo_dist_idfa_1m_am,
       t5.bankCardNo_dist_imei_1m_am,
       t6.bankCardMobile_dist_cid_1m_am,
       t6.bankCardMobile_dist_centNo_1m_am,
       t6.bankCardMobile_dist_bankCardNo_1m_am,
       t6.bankCardMobile_dist_deviceId_1m_am,
       t6.bankCardMobile_dist_idfa_1m_am,
       t6.bankCardMobile_dist_imei_1m_am,

       t7.certno_dist_cid_3m_am,
       t7.certno_dist_mobile_3m_am,
       t7.certno_dist_bankCardNo_3m_am,
       t7.certno_dist_bankCardMobile_3m_am,
       t7.certno_dist_deviceId_3m_am,
       t7.certno_dist_imei_3m_am,
       t7.certno_dist_idfa_3m_am,
       t8.cid_dist_bankCardNo_3m_am,
       t8.cid_dist_bankCardMobile_3m_am,
       t8.cid_dist_deviceId_3m_am,
       t8.cid_dist_idfa_3m_am,
       t8.cid_dist_imei_3m_am,
       t9.mobile_dist_cid_3m_am,
       t9.mobile_dist_certNo_3m_am,
       t9.mobile_dist_bankCardNo_3m_am,
       t9.mobile_dist_deviceId_3m_am,
       t9.mobile_dist_idfa_3m_am,
       t9.mobile_dist_imei_3m_am,
       t10.bankCardNo_dist_cid_3m_am,
       t10.bankCardNo_dist_mobile_3m_am,
       t10.bankCardNo_dist_deviceId_3m_am,
       t10.bankCardNo_dist_idfa_3m_am,
       t10.bankCardNo_dist_imei_3m_am,
       t11.bankCardMobile_dist_cid_3m_am,
       t11.bankCardMobile_dist_centNo_3m_am,
       t11.bankCardMobile_dist_bankCardNo_3m_am,
       t11.bankCardMobile_dist_deviceId_3m_am,
       t11.bankCardMobile_dist_idfa_3m_am,
       t11.bankCardMobile_dist_imei_3m_am
from
    (select event_cid
     from cid_first_loan) t1
left outer join cid_certNo_derived_1m as t2 on t1.event_cid = t2.event_cid
left outer join cid_derived_1m as t3 on t1.event_cid = t3.event_cid
left outer join cid_mobile_derived_1m as t4 on t1.event_cid = t4.event_cid
left outer join cid_bankCardNo_derived_1m as t5 on t1.event_cid = t5.event_cid
left outer join cid_bankCardMobile_derived_1m as t6 on t1.event_cid = t6.event_cid
left outer join cid_certNo_derived_3m as t7 on t1.event_cid = t7.event_cid
left outer join cid_derived_3m as t8 on t1.event_cid = t8.event_cid
left outer join cid_mobile_derived_3m as t9 on t1.event_cid = t9.event_cid
left outer join cid_bankCardNo_derived_3m as t10 on t1.event_cid = t10.event_cid
left outer join cid_bankCardMobile_derived_3m as t11 on t1.event_cid = t11.event_cid;
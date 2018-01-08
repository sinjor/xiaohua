#!/bin/sh
hive -f hive_xiaohua_am_1m.sql
hive -f hive_xiaohua_am_3m.sql
hive -f hive_xiaohua_high_freq_1m.sql
hive -f hive_xiaohua_high_freq_3m.sql
hive -f hive_xiaohua_abnormal.sql
hive -f xiaohua_other.sql
hive -f xiaohua_feature_extract_all.sql

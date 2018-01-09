#!/bin/sh
set -x
table_original=behavior_data_source_useful_flatten_2 #原始数据集
table_train_src=sinjor_train_data_1              #训练集表格
table_test_src=sinjor_test_data_1                #测试集表格
database_train=sinjor_db                         #训练集数据库
database_test=sinjor_db_test                     #测试集数据库
table_feature_dest=xiaohua_feature_extract_all   #合并了label之后的特征变量总表

check_error(){
exitCode=$?
if [ $exitCode -ne 0 ];then
         echo "[ERROR] hive execute failed!"
         exit $exitCode
fi
}

hive -hiveconf train_table=${table_train_src} \
     -hiveconf test_table=${table_test_src} \
     -hiveconf train_database=${database_train} \
     -hiveconf test_database=${database_test} \
     -hiveconf original_table=${table_original} 
     -f divide.sql\



#从训练集中生成通用的中文英文转换码
&&hive -hiveconf source_table=${table_train_src} -hiveconf source_database=${database_train} -f xiaohua_code_from_train.sql\
&&hive -hiveconf source_table=${database_train}.${table_train_src} -hiveconf source_database=${database_test} -f xiaohua_code_from_train.sql\

&&hive -hiveconf source_table=${table_train_src} -hiveconf source_database=${database_train} -f hive_xiaohua_am_1m.sql\
&&hive -hiveconf source_table=${table_train_src} -hiveconf source_database=${database_train} -f hive_xiaohua_am_3m.sql\
&&hive -hiveconf source_table=${table_train_src} -hiveconf source_database=${database_train} -f hive_xiaohua_high_freq_1m.sql\
&&hive -hiveconf source_table=${table_train_src} -hiveconf source_database=${database_train} -f hive_xiaohua_high_freq_3m.sql\
&&hive -hiveconf source_table=${table_train_src} -hiveconf source_database=${database_train} -f hive_xiaohua_abnormal.sql\
&&hive -hiveconf source_table=${table_train_src} -hiveconf source_database=${database_train} -f xiaohua_other.sql\
&&hive -hiveconf source_database=${database_train} -f xiaohua_feature_extract_all.sql\
#导出训练集
&&rm -f ${table_train_src}.csv\
&&hive -e "set hive.cli.print.header=true;select * from ${database_train}.${table_feature_dest};"|\
sed -e 's/'${table_feature_dest}'.//g' -e 's/\t/,/g' >> "${table_train_src}.csv"

&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f hive_xiaohua_am_1m.sql\
&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f hive_xiaohua_am_3m.sql\
&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f hive_xiaohua_high_freq_1m.sql\
&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f hive_xiaohua_high_freq_3m.sql\
&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f hive_xiaohua_abnormal.sql\
&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f xiaohua_other.sql\
&&hive -hiveconf source_database=${database_test} -f xiaohua_feature_extract_all.sql\
&&rm -f ${table_test_src}.csv\
&&hive -e "set hive.cli.print.header=true;select * from ${database_test}.${table_feature_dest};"|\
sed -e 's/'${table_feature_dest}'.//g' -e 's/\t/,/g' >> "${table_test_src}.csv"
check_error
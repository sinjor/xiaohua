#!/bin/sh
set -x

database_original=default                        #原始数据所在数据库
database_dest=sinjor_db_time                     #中间数据所在数据库
database_label=default                           #label所在数据库

table_original=behavior_data_source_useful_flatten_2 #原始数据集
table_feature_dest=xiaohua_feature_extract_all   #合并了label之后的特征变量总表
table_label=ml_labels                            #labe数据集

table_train_data=sinjor_train_data_rand
table_test_data=sinjor_test_data_rand

check_error(){
exitCode=$?
if [ $exitCode -ne 0 ];then
         echo "[ERROR] hive execute failed!"
         exit $exitCode
fi
}

#数据处理
data_processing()
{
#  echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f hive_xiaohua_am_1m.sql&&\
#&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f hive_xiaohua_am_3m.sql&&\
#&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f hive_xiaohua_high_freq_1m.sql&&\
#&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f hive_xiaohua_high_freq_3m.sql&&\
#&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f hive_xiaohua_abnormal.sql&&\
#&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f xiaohua_other.sql&&\
#echo "line:${LINENO}"&&hive -hiveconf source_database=${database_src}\
#                              -hiveconf label_table=${table_label}\
#                              -hiveconf label_database=${database_label}\
#                              -hiveconf train_table=${table_train_data}\
#                              -hiveconf test_table=${table_test_data}\
#                              -hiveconf feature_table=${table_feature_dest} -f xiaohua_feature_extract_all.sql&&\
echo "line:${LINENO}"&&rm -f ${table_train_data}.csv&&\
echo "line:${LINENO}"&&hive -e "set hive.cli.print.header=true;select * from ${database_src}.${table_train_data};"|\
sed -e 's/'${table_feature_dest}'.//g' -e 's/\t/,/g' >> "${table_train_data}.csv"&&\

echo "line:${LINENO}"&&rm -f ${table_test_data}.csv&&\
echo "line:${LINENO}"&&hive -e "set hive.cli.print.header=true;select * from ${database_src}.${table_test_data};"|\
sed -e 's/'${table_feature_dest}'.//g' -e 's/\t/,/g' >> "${table_test_data}.csv"
hive -e "set hive.cli.print.header=true;select * from sinjor_train_data_time;" | sed -e 's/sinjor_train_data_time.//g' -e 's/\t/,/g' >>sinjor_train_data_time.csv
}

#数据不易，且删且珍惜
#hive -e "drop database if exists ${database_dest} cascade;create database ${database_dest};"&&\
#echo "line:${LINENO}"&&hive -hiveconf source_table=${database_original}.${table_original} -hiveconf source_database=${database_dest} -f xiaohua_code_from_train.sql&&\
(table_src=${database_original}.${table_original};database_src=${database_dest};data_processing)&&\
echo "data processing success"||\
check_error
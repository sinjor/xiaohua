#!/bin/sh
set -x
table_original=behavior_data_source_useful_flatten_2 #原始数据集
table_train_src=sinjor_train_data_1              #训练集表格
table_test_src=sinjor_test_data_1                #测试集表格
database_train=sinjor_db_train                   #训练集数据库
database_test=sinjor_db_test                     #测试集数据库
table_feature_dest=xiaohua_feature_extract_all   #合并了label之后的特征变量总表
database_label=default                           #label所在数据库
database_original=default                        #原始数据所在数据库
table_label=ml_labels                            #labe数据集

check_error(){
exitCode=$?
if [ $exitCode -ne 0 ];then
         echo "[ERROR] hive execute failed!"
         exit $exitCode
fi
}
#从训练集中生成通用的中文英文转换码
commom_transfrom()
{
echo "line:${LINENO}"&&hive -hiveconf source_table=${table_train_src} -hiveconf source_database=${database_train} -f xiaohua_code_from_train.sql\
&&echo "line:${LINENO}"&&hive -hiveconf source_table=${database_train}.${table_train_src} -hiveconf source_database=${database_test} -f xiaohua_code_from_train.sql
}

data_processing()
{
  echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f hive_xiaohua_am_1m.sql\
&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f hive_xiaohua_am_3m.sql\
&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f hive_xiaohua_high_freq_1m.sql\
&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f hive_xiaohua_high_freq_3m.sql\
&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f hive_xiaohua_abnormal.sql\
&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_src} -hiveconf source_database=${database_src} -f xiaohua_other.sql\
&&echo "line:${LINENO}"&&hive -hiveconf source_database=${database_src} -hiveconf label_table=${table_label} -hiveconf label_database=${database_label} -f xiaohua_feature_extract_all.sql\
&&echo "line:${LINENO}"&&rm -f ${table_src}.csv\
&&echo "line:${LINENO}"&&hive -e "set hive.cli.print.header=true;select * from ${database_src}.${table_feature_dest};"|\
sed -e 's/'${table_feature_dest}'.//g' -e 's/\t/,/g' >> "${table_src}.csv"

}

#划分训练集和测试集
hive -hiveconf train_table=${table_train_src} \
     -hiveconf test_table=${table_test_src} \
     -hiveconf train_database=${database_train} \
     -hiveconf test_database=${database_test} \
     -hiveconf original_table=${database_original}.${table_original} -f divide.sql\
&&commom_transfrom\
&&(table_src=${table_train_src};database_src=${database_train};data_processing)\
&&(table_src=${table_test_src};database_src=${database_test};data_processing)\
&&echo "data processing success"\
||check_error

#&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f hive_xiaohua_am_1m.sql\
#&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f hive_xiaohua_am_3m.sql\
#&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f hive_xiaohua_high_freq_1m.sql\
#&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f hive_xiaohua_high_freq_3m.sql\
#&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f hive_xiaohua_abnormal.sql\
#&&echo "line:${LINENO}"&&hive -hiveconf source_table=${table_test_src} -hiveconf source_database=${database_test} -f xiaohua_other.sql\
#&&echo "line:${LINENO}"&&hive -hiveconf source_database=${database_test} -hiveconf label_table=${table_label} -hiveconf label_database=${database_label} -f xiaohua_feature_extract_all.sql\
#&&echo "line:${LINENO}"&&rm -f ${table_test_src}.csv\
#&&echo "line:${LINENO}"&&hive -e "set hive.cli.print.header=true;select * from ${database_test}.${table_feature_dest};"|\
#sed -e 's/'${table_feature_dest}'.//g' -e 's/\t/,/g' >> "${table_test_src}.csv"

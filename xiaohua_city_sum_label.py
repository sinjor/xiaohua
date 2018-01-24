#根据城市个数对城市分类归一
import pandas as pd 
#pd.set_option('display.max_rows',None)

input_train_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_train_data_rand_v3.csv", dtype = "str")
input_test_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_test_data_rand_v3.csv", dtype = "str")

train_data = input_train_data.loc[:, ["event_mobilecity_code","label"]].astype(int)

input_train_data.loc[:, ["event_mobilecity_code","label"]] = input_train_data.loc[:, ["event_mobilecity_code","label"]].astype(int)
input_test_data.loc[:, ["event_mobilecity_code","label"]] = input_test_data.loc[:, ["event_mobilecity_code","label"]].astype(int)

#print(train_data.info())
event_mobilecity_code_group = train_data[["event_mobilecity_code", "label"]].groupby(["event_mobilecity_code", "label"])["label"].count()
#groupby.count()返回的是series 索引为 event_mobilecity_code 和label的组合，因此iloc仍然为series的绝对顺序，loc则有两个索引来取值，label的值只有0和1
event_mobilecity_code_summary = event_mobilecity_code_group.loc[:,0].to_frame(name = "label_0")
event_mobilecity_code_summary = pd.concat([event_mobilecity_code_summary,event_mobilecity_code_group.loc[:,1].rename("label_1")], axis = 1, join = "outer")
#不能直接赋值，event_mobilecity_code_summary的索引会缺失
#event_mobilecity_code_summary["label_1"] = event_mobilecity_code_group.loc[:,1] 
event_mobilecity_code_summary.fillna(value = 0, inplace = True)
event_mobilecity_code_summary = event_mobilecity_code_summary.astype("int64")
event_mobilecity_code_summary["label_0_1_sum"] = event_mobilecity_code_summary.apply(lambda x: x.loc["label_0"] + x.loc["label_1"], axis = 1)
event_mobilecity_code_summary["proportion"] = event_mobilecity_code_summary.apply(lambda x: x.loc["label_0_1_sum"] /69999, axis = 1)
event_mobilecity_code_summary["label_1_div_all"] = event_mobilecity_code_summary.apply(lambda x: x.loc["label_1"] / x.loc["label_0_1_sum"], axis = 1)
event_mobilecity_code_summary["label_1_div_data"] = event_mobilecity_code_summary.apply(lambda x: (x.loc["proportion"] * x.loc["label_1_div_all"]), axis = 1)

#event_mobilecity_code_summary.sort_values("label_0_1_sum", ascending = False, inplace = True)
event_mobilecity_code_summary.reset_index(inplace = True)
event_mobilecity_code_summary["sort_id"] = event_mobilecity_code_summary.label_0_1_sum.rank(ascending = False, method = "min")
event_mobilecity_code_summary["event_mobilecity_code_sum_flag"] = event_mobilecity_code_summary.apply(lambda x: int(x.loc["sort_id"]/16), axis =1)
event_mobilecity_code_summary.loc[event_mobilecity_code_summary.loc[:,"event_mobilecity_code_sum_flag"]>5, "event_mobilecity_code_sum_flag"] = 5
def label_flag_func (x):
	if x.loc["sort_id"] < 80:
		return int(x.loc["label_prop_sort_id"]/16)
	else:
		return 5

event_mobilecity_code_summary["label_prop_sort_id"] = event_mobilecity_code_summary.label_1_div_data[event_mobilecity_code_summary.sort_id <80].rank(ascending = False, method = "min")
event_mobilecity_code_summary["event_mobilecity_code_label_flag"] = event_mobilecity_code_summary.apply(label_flag_func, axis =1)
event_mobilecity_code_feture = event_mobilecity_code_summary[["event_mobilecity_code", "event_mobilecity_code_sum_flag", "event_mobilecity_code_label_flag"]]
print(event_mobilecity_code_feture.event_mobilecity_code_label_flag.head())
input_train_data = pd.merge(input_train_data, event_mobilecity_code_feture, how = "left", on = "event_mobilecity_code")
input_test_data = pd.merge(input_test_data, event_mobilecity_code_feture, how = "left", on = "event_mobilecity_code")
input_train_data.to_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_train_data_rand_v3_b1.csv",na_rep = "NULL")
input_test_data.to_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_test_data_rand_v3_b1.csv",na_rep = "NULL")
exit()
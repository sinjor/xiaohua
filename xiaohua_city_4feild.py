#城市根据逾期率和逾期绝对数划分4象限
import pandas as pd 
import matplotlib.pyplot as plt
#pd.set_option('display.max_rows',None)

input_train_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_train_data_rand_v3.csv", dtype = "str")
input_test_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_test_data_rand_v3.csv", dtype = "str")

train_data = input_train_data.loc[:, ["event_mobilecity_code","label"]].astype(int)
#train_data.label = train_data.label.astype(int)
input_train_data.loc[:, ["event_mobilecity_code","label"]] = input_train_data.loc[:, ["event_mobilecity_code","label"]].astype(int)
input_test_data.loc[:, ["event_mobilecity_code","label"]] = input_test_data.loc[:, ["event_mobilecity_code","label"]].astype(int)

#print(train_data.info())
event_mobilecity_code_group = train_data[["event_mobilecity_code", "label"]].groupby(["event_mobilecity_code", "label"])["label"].count()
event_mobilecity_code_summary = event_mobilecity_code_group.loc[:,0].to_frame(name = "label_0")
event_mobilecity_code_summary = pd.concat([event_mobilecity_code_summary,event_mobilecity_code_group.loc[:,1].rename("label_1")], axis = 1, join = "outer")
#event_mobilecity_code_summary["label_1"] = event_mobilecity_code_group.loc[:,1] #不能直接赋值，event_mobilecity_code_summary的索引会缺失
event_mobilecity_code_summary.fillna(value = 1, inplace = True)
event_mobilecity_code_summary = event_mobilecity_code_summary.astype("int64")
event_mobilecity_code_summary["label_0_1_sum"] = event_mobilecity_code_summary.apply(lambda x: x.loc["label_0"] + x.loc["label_1"], axis = 1)
event_mobilecity_code_summary["proportion"] = event_mobilecity_code_summary.apply(lambda x: x.loc["label_0_1_sum"] /69999, axis = 1)
event_mobilecity_code_summary["label_1_div_all"] = event_mobilecity_code_summary.apply(lambda x: x.loc["label_1"] / x.loc["label_0_1_sum"], axis = 1)
event_mobilecity_code_summary["label_1_div_data"] = event_mobilecity_code_summary.apply(lambda x: (x.loc["proportion"] * x.loc["label_1_div_all"]), axis = 1)

def label_1_flag_func (x):
	if (x.loc["label_1"] < 10) & (x.loc["label_1_div_all"] < 0.05) :
		return 0
	elif (x.loc["label_1"] < 10) & (x.loc["label_1_div_all"] >= 0.05) :
		return 1
	elif (x.loc["label_1"] >= 10) & (x.loc["label_1_div_all"] < 0.05) :
		return 2
	elif (x.loc["label_1"] >= 10) & (x.loc["label_1_div_all"] >= 0.05) :
		return 3
	else:
		return 4
print(event_mobilecity_code_summary.info())
event_mobilecity_code_summary["event_mobilecity_code_4_flag"] = event_mobilecity_code_summary.apply(label_1_flag_func, axis = 1)
print(event_mobilecity_code_summary.event_mobilecity_code_4_flag.value_counts())

event_mobilecity_code_summary.reset_index(inplace = True)
event_mobilecity_code_feture = event_mobilecity_code_summary[["event_mobilecity_code", "event_mobilecity_code_4_flag"]]

input_train_data = pd.merge(input_train_data, event_mobilecity_code_feture, how = "left", on = "event_mobilecity_code")
input_test_data = pd.merge(input_test_data, event_mobilecity_code_feture, how = "left", on = "event_mobilecity_code")
input_train_data.to_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_train_data_rand_v3_b2.csv",na_rep = "NULL")
input_test_data.to_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_test_data_rand_v3_b2.csv",na_rep = "NULL")

exit()
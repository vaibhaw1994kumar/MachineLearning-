import pandas as pd 
import numpy as np
import h2o
h2o.init()
data1 = h2o.upload_file("/Users/mac/Desktop/versa-application/oct/october 1.csv")
data2 = h2o.upload_file("/Users/mac/Desktop/versa-application/oct/october 2.csv")
data3 = h2o.upload_file("/Users/mac/Desktop/versa-application/oct/october 3.csv")
data4 = h2o.upload_file("/Users/mac/Desktop/versa-application/oct/october 4.csv")
data5 = h2o.upload_file("/Users/mac/Desktop/versa-application/oct/october 5.csv")
data6 = h2o.upload_file("/Users/mac/Desktop/versa-application/oct/Oct 6.csv")
data = h2o.upload_file("/Users/mac/Desktop/versa-application/oct/TM OCt (23-30).csv")

data.set_names(["Receive time","Appliance","Source address","Destination address","Source Port","Destination port","Protocol","Application","Type","Rule","Egress interface","Ingress interface","From Zone","To Zone","Source country","Destination country","User","Packets Received","Packets Sent","Received Bytes","Transmitted Bytes","Session duration","Reverse Rate (Bps)","Forward Rate (Bps)"])
data5.set_names(["Receive time","Appliance","Source address","Destination address","Source Port","Destination port","Protocol","Application","Type","Rule","Egress interface","Ingress interface","From Zone","To Zone","Source country","Destination country","User","Packets Received","Packets Sent","Received Bytes","Transmitted Bytes","Session duration","Reverse Rate (Bps)","Forward Rate (Bps)"])
data6.set_names(["Receive time","Appliance","Source address","Destination address","Source Port","Destination port","Protocol","Application","Type","Rule","Egress interface","Ingress interface","From Zone","To Zone","Source country","Destination country","User","Packets Received","Packets Sent","Received Bytes","Transmitted Bytes","Session duration","Reverse Rate (Bps)","Forward Rate (Bps)"])
data10 = h2o.upload_file("/Users/mac/Desktop/versa-application/oct/october 10.csv")
data10.set_names(["Receive time","Appliance","Source address","Destination address","Source Port","Destination port","Protocol","Application","Type","Rule","Egress interface","Ingress interface","From Zone","To Zone","Source country","Destination country","User","Packets Received","Packets Sent","Received Bytes","Transmitted Bytes","Session duration","Reverse Rate (Bps)","Forward Rate (Bps)"])

df = data.as_data_frame()
df1 = data1.as_data_frame()
df2 = data2.as_data_frame()
df3 = data3.as_data_frame()
df4 = data4.as_data_frame()
df5 = data5.as_data_frame()
df6 = data6.as_data_frame()
df10 = data10.as_data_frame()

frames = [df,df1,df2,df3,df4,df5,df6,df10]
result = pd.concat(frames)
result.info()

hf = h2o.H2OFrame(result)
df_log = hf.as_data_frame()
df_log["Receive time"] = df_log['Receive time'].map(lambda x: x.rstrip('Z'))

df_log = df_log[df_log.Application != "unknown_tcp"]
df_log = df_log[df_log.Application != "unknown_udp"]
df_log = df_log[df_log.Application != "dns"]

df_log.info()
df_log2=pd.DataFrame([x.split('T') for x in df_log['Receive time'].tolist()],columns= ['Receive date','Receive time1'])
df_log = pd.concat([df_log, df_log2], axis=1)
df_log.head()
df_log.drop("Receive time",axis=1,inplace=True)
df_log['Receive time2'] = df_log['Receive date'].map(str)+ " " + df_log['Receive time1'].map(str)
df_log.dropna(subset = ['Appliance','Source address','Destination address','Protocol','Application','Rule','From Zone','To Zone','Source country','Destination country','Packets Received','Packets Sent','Received Bytes','Transmitted Bytes','Session duration','Reverse Rate (Bps)','Forward Rate (Bps)'],how='all',inplace=True)

df_log.info()

cols = ["Source Port","Destination port","Type","Egress interface","Ingress interface","User","Receive date","Receive time1"]
df_log.drop(cols,axis=1,inplace=True)

hf1 = h2o.H2OFrame(df_log)
hf1.describe()

df_log["Forward Rate (Bps)"].corr(df_log["Transmitted Bytes"])
df_log["Forward Rate (Bps)"].corr(df_log["Session duration"])
df_log["Processed Bytes (Bps)"] = df_log["Received Bytes"] + df_log["Transmitted Bytes"]
df_log.head()

cols = ['Received Bytes','Transmitted Bytes']
df_log.drop(cols,axis=1,inplace=True)

df_log["Forward Rate (Bps)"].corr(df_log["Processed Bytes (Bps)"])
df_log["Session duration"].corr(df_log["Processed Bytes (Bps)"])
df_log["Packets Sent"].corr(df_log["Session duration"])
df_log["Packets Received"].corr(df_log["Session duration"])
df_log["Packets Processed"] = df_log["Packets Received"] + df_log["Packets Sent"]
df_log["Packets Processed"].corr(df_log["Session duration"])
df_log.drop("Packets Processed",axis=1,inplace=True)

df_log["Application"].value_counts()

import matplotlib.pyplot as plt
import pylab
plt.scatter(df_log["Processed Bytes (Bps)"],df_log["Session duration"])

df_log = df_log[df_log.Application != "snmp"]
df_log["Appliance"].value_counts()
df_log.drop("Appliance",axis=1,inplace=True)

df_log.head()
df_log.info()

df_log["Rule"].value_counts()
df_log.drop("Rule",axis=1,inplace=True)
df_log["Protocol"].value_counts()
df_log["From Zone"].value_counts()
df_log["Source country"].value_counts()
df_log["Source address"].value_counts()
df_log["Destination address"].value_counts()

df_log.isnull().any().any()
df_log.isnull().sum()

import missingno as msno
msno.matrix(df_log)
df_log["Source country"].value_counts()
msno.bar(df_log.sample(1000))
msno.heatmap(df_log)

from pandas.tools.plotting import scatter_matrix
attributes = ['Packets Received','Packets Sent','Session duration','Reverse Rate (Bps)','Forward Rate (Bps)','Processed Bytes (Bps)']
scatter_matrix(df_log[attributes],figsize=(12,8))

import matplotlib.pyplot as plt
import seaborn as sns
plt.hist(df_log['Session duration2'], color = 'blue', edgecolor = 'black',
         bins = int(180/5))
         
sns.distplot(df_log['Session duration'], hist=True, kde=False, 
             bins=int(180/5), color = 'blue',
             hist_kws={'edgecolor':'black'})

sns.distplot(df_log['Application'], hist=True, kde=False, 
             bins=int(180/5), color = 'blue',
             hist_kws={'edgecolor':'black'})
             
from sklearn.preprocessing import LabelEncoder
encoder = LabelEncoder()
df_log_app = df_log["Application"]
df_log_app_encoded = encoder.fit_transform(df_log_app)
df_log_app_encoded

print(encoder.classes_)

from sklearn.preprocessing import OneHotEncoder
encoder = OneHotEncoder()
df_log_app_1hot = encoder.fit_transform(df_log_app_encoded.reshape(-1,1))
df_log_app_1hot

df_log_app_1hot.toarray()

from sklearn.preprocessing import LabelBinarizer
encoder = LabelBinarizer()
df_log_app_1hot = encoder.fit_transform(df_log_app)
df_log_app_1hot

from sklearn.base import BaseEstimator, TransformerMixin






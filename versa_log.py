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
hf.describe()
df_log = hf.as_data_frame()

%matplotlib inline
import matplotlib.pyplot as plt
df_log["Application"].value_counts().hist(bins=50,figsize=(20,15))

df_log.hist(bins=50,figsize=(20,15))

ds2 = hf.group_by("Application").count().get_frame()
ds2.sort(1, ascending=False)

df_log = df_log[df_log.Application != "unknown_tcp"]
df_log.info()

cols = ["Source Port","Destination port","Type","Egress interface","Ingress interface","User"]
hf = hf.drop(cols)

df_log = hf.as_data_frame()
df_log = df_log[df_log.Application != "unknown_tcp"]
df_log.info()

ds2 = hf.group_by("Application").count().get_frame()
ds2.sort(1, ascending=False)

hf1 = h2o.H2OFrame(df_log)
ds2 = hf1.group_by("Application").count().get_frame()
ds2.sort(1, ascending=False)

df_log1 = hf1.as_data_frame()
df_log1 = df_log1[df_log1.Application != "unknown_udp"]
df_log1.info()

hf2 = h2o.H2OFrame(df_log1)
hf2.describe()
%matplotlib inline
import matplotlib.pyplot as plt
df_log1["Application"].value_counts().hist(bins=50,figsize=(20,15))
df_log1 = df_log1[df_log1.Application != "dns"]
df_log1.info()
hf2 = h2o.H2OFrame(df_log1)
ds2 = hf2.group_by("Application").count().get_frame()
ds2.sort(1, ascending=False)

%matplotlib inline
import matplotlib.pyplot as plt
df_log1["Application"].value_counts().hist(bins=50,figsize=(20,15))

df_log1.head()
%matplotlib inline
import matplotlib.pyplot as plt
df_log1["Source country"].value_counts().hist(bins=50,figsize=(20,15))
hf2.describe()

df_log1["Source country"].value_counts()
df_log1.info()

df_log2=pd.DataFrame([x.split('T') for x in df_log1['Receive time'].tolist()],columns= ['Receive date','Receive time1'])
df_log1 = pd.concat([df_log1, df_log2], axis=1)
hf_log2 = h2o.H2OFrame(df_log1)
hf_log2.describe()

hf_log2 = h2o.H2OFrame(df_log1)
hf_log2["Receive time1"].rstrip(set='Z')
hf_log2.describe()

hf_log2["Receive time1"] = hf_log2["Receive time1"].rstrip(set='Z')
hf_log2.describe()

hf_log2['Receive date'] = hf_log2['Receive time'].lstrip('T').rstrip('Z')
hf_log2.describe()

df_log1.drop("Receive date",axis=1,inplace=True)
df_log1.drop("Receive time1",axis=1,inplace=True)

df_log1.head()
df_log1.head()

df_new_log = df[df['Receive time'].notnull()]

df_log2=pd.DataFrame([x.split('T') for x in df_new_log['Receive time'].tolist()],columns= ['Receive date','Receive time1'])
df_log1 = pd.concat([df_log1, df_log2], axis=1)
df_log1.head()
hf_new_log = h2o.H2OFrame(df_log1)


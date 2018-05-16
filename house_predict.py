import h2o
import pandas as pd
import numpy as np
import scipy.stats  as stats
import matplotlib.pyplot as plt
h2o.init()

data = h2o.upload_file("/Users/mac/Desktop/ENB2012_data.csv")
data.describe()

factorList = ["X6","X8"]
data[factorList] = data[factorList].asfactor()
train,test = data.split_frame([0.8])
x = ["X1","X2","X3","X4","X5","X6","X7","X8"]
Y = ["Y2"]

train.describe()
train.cor().round(2)

res = train[x].cor(train[y]).as_data_frame()

res.index = x
res.plot.barh()
plt.show()

m = h2o.estimators.deeplearning.H2ODeepLearningEstimator()
for i in range(len(y)):
    m.train(x,y[i],train)

p = m.predict(test)
"cost error: {}".format(m.mse())

p.as_data_frame()
p["predict"].cbind(test["Y2"]).as_data_frame()

(p["predict"] == test["Y2"]).mean()
m.model_performance(test)

data["X1"].hist()
data["X2"].hist()
data["X3"].hist()
data["X4"].hist()
data["X5"].hist()
data["X7"].hist()
data["Y1"].hist()
data["Y2"].hist()

df = data.as_data_frame()
df.plot(kind="density",figsize=(9,9),xlim=(-1,11))

df1 = df.filter(['X1','Y2'], axis=1)
df1.plot(kind="density",figsize=(100,100),xlim=(-1,1),Ylim=-1,1)

df1 = df.filter(['X1','Y2'], axis=1)
df1.plot(kind="density",figsize=(100,100),xlim=(-10,10))

df1 = df.filter(['X1','Y2'], axis=1)
df1.plot(kind="density",figsize=(10,10),xlim=(-10,10))

df1 = df.filter(['X1','Y2'], axis=1)
df1.plot(kind="density",figsize=(10,10),xlim=(-10,10))

df1 = df.filter(['X1','Y2'], axis=1)
df1.plot(kind="density",figsize=(10,10),xlim=(-1,1))

df = data.as_data_frame()
df.plot(kind="density",figsize=(9,9),xlim=(-1,1))

df1 = df.filter(['Y2'], axis=1)
df1.plot(kind="density",figsize=(9,9),xlim=(-1,100))

df1 = df.filter(['Y2'], axis=1)
df1.plot(kind="density",figsize=(9,9),xlim=(-1,100))

data["Y2"] = data["Y2"]/25
data["X2"] = data["X2"]/600
data["X3"] = data["X3"]/300
data["X4"] = data["X4"]/150
data["X5"] = data["X5"]/3
data["Y1"] = data["Y1"]/20

data.describe()

df = data.as_data_frame()
df.plot(kind="density",figsize=(9,9),xlim=(-1,5))




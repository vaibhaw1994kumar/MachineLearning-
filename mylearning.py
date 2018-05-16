import h2o
h2o.init()

#datasets="https://gist.github.com/curran/a08a1080b88344b0c8a7"
data = h2o.upload_file("/Users/mac/Downloads/Versa Analytics Traffic.csv")
data.describe()
data = data[:,1:]
y = "Destination address"
x = data.names
x.remove(y)
train,test = data.split_frame([0.8])

m = h2o.estimators.deeplearning.H2ODeepLearningEstimator()
m.train(x,y,train)
print("##################################################################")
p = m.predict(test)
print(p)

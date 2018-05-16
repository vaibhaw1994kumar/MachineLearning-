import pandas as pd
import numpy
import matplotlib
import h2o

h2o.init()
data = h2o.upload_file("/Users/mac/Downloads/mydata/iris.csv")
data.describe()
y = 'species'
x = data.names
x.remove(y)
print(x)

train,test = data.split_frame([0.8])
m = h2o.estimators.deeplearning.H2ODeepLearningEstimator()
m.train(x,y,train)

p = m.predict(test)
print("##############################################################")
print(p)
"cost error: {}".format(m.mse())
##confusion matrix..for more info https://machinelearningmastery.com/confusion-matrix-machine-learning/
m.confusion_matrix(test)
###p.print() shows only 10 rows..for downloading all prediction..install pandas
p.as_data_frame()
##compare predicted species and existing species
p["predict"].cbind(test["species"]).as_data_frame()
###percentage of correct predictions
(p["predict"] == test["species"]).mean()
m.model_performance(test)

data.quantile()
data.dim ##no of rows&cols
data.nrow 
data.ncols 
data.levels()

data["petal_length"]=data["petal_length"]*1.2
data["ratio"]=data["petal_width"]/data["sepal_width"] ##adding a new col ratio
data.describe()
data["petal_length"].sd()
data["ratio"].cor(data["petal_length"]) ### correlation of ratio and petal-length
data["islong"]=(data["petal_length"]>data["petal_length"].mean()[0]).ifelse(1,0) ### creating new column is_long petal len grater than mean petal len
data.quantile() 
###data.group_by("species").count().mean("petal_length").frame ###calculate mean petal-len for each species
data.group_by("species").count().mean("petal_length").sum("islong").frame ###how many in each category is greater than mean petal-len
data["petal_length"].sum()
data["petal_length"].var()

###plot the histogram for sepal_len,petal_len,sepal_width,petal_width,how each feature is varying across the sample
data["petal_length"].hist()
data["petal_width"].hist()
data["sepal_length"].hist()
data["sepal_width"].hist()

###install pandas and create a pandas dataframe from h2o
df=data.as_data_frame()
df.info()

df.corr(method="spearman").round(2) ## getting pairwise correlation between columns plz refer https://www.datascience.com/blog/introduction-to-correlation-learn-data-science-tutorials
### addition of new cols with less cost
##ratio_frame=data["petal_width"]/data["sepal_width"]
##ratio_frame.clo_names=["ratio"]
##data=data.cbind(ratio_frame)
##data=h2o.assign(data,"iris")
##ratio_frame.remove()
df1 = pd.DataFrame({'petal_length':[2,3,4,5],'price':[4,5.5,8,10]})
prices=h2o.H2OFrame(df1)
data["petal_length"]=data["petal_length"].round()###integerizing a petal-len col i.e H2o merge accepts only integer values
iris_prices=data.merge(prices)









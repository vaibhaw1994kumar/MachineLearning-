import h2o
import imp
import math as math
from h2o.estimators.kmeans import H2OKMeansEstimator
h2o.init()

data = h2o.upload_file("/Users/mac/Downloads/versa.csv")
df = data.as_data_frame()
df.head()
data.describe()

cols = ['Flow Key','Type','Rule','Source country','Destination country','User','C26']
df.drop(cols, inplace=True, axis=1)
df.info()
hf = h2o.H2OFrame(df)
hf.describe()


try:
    imp.find_module('pandas')
    can_pandas = True
    import pandas as pd
except:
    can_pandas = False
    
try:
    imp.find_module('seaborn')
    can_seaborn = True
    import seaborn as sns
except:
    can_seaborn = False

%matplotlib inline

if can_seaborn:
    sns.set()
    if can_seaborn:
        sns.set_context("notebook")
        sns.pairplot(hf.as_data_frame(True), vars=["Receive time", "Appliance", "Source address", "Destination address","Source Port","Destination port","Protocol","Egress interface","Ingress interface","From Zone","To Zone","Packets Received","Packets Sent","Received Bytes","Transmitted Bytes","Session duration","Reverse Rate (Bps)","Forward Rate (Bps)"], hue="Application");

results = [H2OKMeansEstimator(k=clusters, init="Random", seed=2, standardize=True) for clusters in range(2,13)]
for estimator in results:
    estimator.train(x=hf.col_names[7], training_frame = hf)
       
def diagnostics_from_clusteringmodel(model):
    total_within_sumofsquares = model.tot_withinss()
    number_of_clusters = len(model.centers())
    number_of_dimensions = len(model.centers()[0])
    number_of_rows = sum(model.size())
    aic = total_within_sumofsquares + 2 * number_of_dimensions * number_of_clusters
    bic = total_within_sumofsquares + math.log(number_of_rows) * number_of_dimensions * number_of_clusters
    return {'Clusters':number_of_clusters,
    'Total Within SS':total_within_sumofsquares, 'AIC':aic, BIC':bic}
    if can_pandas:
        diagnostics = pd.DataFrame( [diagnostics_from_clusteringmodel(model) for model in results])
        diagnostics.set_index('Clusters', inplace=True)
    if can_pandas:
        diagnostics.plot(kind='line');
    print(diagnostics)
        
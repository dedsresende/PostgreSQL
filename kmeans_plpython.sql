CREATE OR REPLACE FUNCTION tm_kmeans (table_name TEXT, cols TEXT[], id TEXT, n_clusters INT)
RETURNS SETOF TEXT[]
AS 
$$
import numpy as np
from sklearn.cluster import KMeans
import sklearn.preprocessing as preprocessing

a = [table_name]
b = cols
c = [id]

myList = []

queryLine = []

for t in b:
    q = "(CASE WHEN %s::TEXT IS NULL THEN '0' ELSE %s::TEXT END) AS %s"%(t,t,t)
    queryLine.append(q)

queryLine = [", ".join(queryLine)]
queryPlaceholder = tuple(c+queryLine+a)

query = plpy.execute("SELECT %s, %s FROM %s"%queryPlaceholder)
plpy.notice("SELECT %s, %s FROM %s"%queryPlaceholder)

for col in cols:
    colVals = [i[col] for i in query]
    myList.append(colVals)

dfPK = [str(x[id]) for x in query]
plpy.notice(dfPK)

df = np.array(myList,dtype=object)
df = np.transpose(df)
df = preprocessing.scale(df)

plpy.notice(df.shape)

dbkmeans = KMeans(n_clusters=n_clusters, init="k-means++", n_init=10, max_iter=300, tol=0.0001, precompute_distances="auto", verbose=0, random_state=None, copy_x=True, n_jobs=1)
km = dbkmeans.fit_predict(df)

res = np.array([dfPK,km],dtype=object)
res = np.transpose(res)
plpy.notice(res)

return res
$$
LANGUAGE plpython2u;

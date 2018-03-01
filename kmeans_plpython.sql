--First step is to create a data type as a multi column table which the columns are the geometry column and the k-means label column
CREATE TYPE kmeans_table AS (geom GEOMETRY, km_lbl TEXT);

--Function
CREATE OR REPLACE FUNCTION tm_kmeans (table_name TEXT, cols TEXT[], geom TEXT, n_clusters INT)

RETURNS SETOF kmeans_table
AS 
$$
--Used libraries
from plpy import spiexceptions
from sklearn.cluster import KMeans
import sklearn.preprocessing as preprocessing
import numpy as np

--Variables as lists
a = [table_name]
b = cols
c = [geom]
myList = []
queryLine = []

--Build query fot the cluster algorithm
for t in b:
    q = "(CASE WHEN %s::TEXT IS NULL THEN '0' ELSE %s::TEXT END) AS %s"%(t,t,t)
    queryLine.append(q)

queryLine = [", ".join(queryLine)]
queryPlaceholder = tuple(c+queryLine+a)

query = plpy.execute("SELECT %s as g, %s FROM %s"%queryPlaceholder)
plpy.notice("SELECT %s as g, %s FROM %s"%queryPlaceholder)

--Build array and running the cluster algorithm
for col in cols:
    colVals = [i[col] for i in query]
    myList.append(colVals)

geomCol = [x['g'] for x in query]

df = np.array(myList,dtype=object)
df = np.transpose(df)
df = preprocessing.scale(df)
plpy.notice(df.shape)

dbkmeans = KMeans(n_clusters=n_clusters, init="k-means++", n_init=10, max_iter=300, tol=0.0001, precompute_distances="auto", verbose=0, random_state=None, copy_x=True, n_jobs=1)
km = dbkmeans.fit_predict(df)

--This line can make the code slower but is the one the guarantees the code to return a table and not a single row
for g, k in zip(geomCol, km):
    yield (g, k)

$$
LANGUAGE plpython2u;

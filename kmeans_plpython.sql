CREATE OR REPLACE FUNCTION tm_kmeans (table_name TEXT, cols TEXT[], geom TEXT, n_clusters INT)

RETURNS TEXT
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

query = plpy.execute("SELECT ST_AsText(%s) as g, %s FROM %s"%queryPlaceholder)
plpy.notice("SELECT ST_AsText(%s) as g, %s FROM %s"%queryPlaceholder)

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

--Build update query using geometry as text as anchor for the kmeans label column merge
tab = plpy.execute('SELECT * FROM %s'%table_name)
if 'km_lbl' not in tab.colnames():
	plpy.execute('ALTER TABLE %s ADD COLUMN km_lbl TEXT'%table_name)

res = np.array([geomCol,km],dtype=object)
res = np.transpose(res)

plan = plpy.prepare('UPDATE %s SET km_lbl = $1 WHERE ST_AsText(%s) = $2'%(table_name,geom), ["text", "text"])
plpy.execute(plan, [res[1], res[0]])

ret_message = 'km_lbl updated with %s clusters at %s'%(n_clusters,table_name)
return ret_message
plpy.notice(ret_message)

$$
LANGUAGE plpython2u;

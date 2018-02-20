CREATE OR REPLACE FUNCTION tm_kmeans (table_name TEXT, cols TEXT[], geom TEXT, n_clusters INT)
RETURNS TEXT
AS 
$$
import numpy as np
from sklearn.cluster import KMeans
import sklearn.preprocessing as preprocessing
from plpy import spiexceptions

a = [table_name]
b = cols
c = [geom]

myList = []

queryLine = []

for t in b:
    q = "(CASE WHEN %s::TEXT IS NULL THEN '0' ELSE %s::TEXT END) AS %s"%(t,t,t)
    queryLine.append(q)

queryLine = [", ".join(queryLine)]
queryPlaceholder = tuple(c+queryLine+a)

query = plpy.execute("SELECT ST_AsText(%s) as g, %s FROM %s"%queryPlaceholder)
plpy.notice("SELECT ST_AsText(%s) as g, %s FROM %s"%queryPlaceholder)

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

tab = plpy.execute('SELECT * FROM %s'%table_name)
if 'km_lbl' not in tab.colnames():
	plpy.execute('ALTER TABLE %s ADD COLUMN km_lbl INT'%table_name)

plan = plpy.prepare('UPDATE %s SET km_lbl = $1 WHERE ST_AsText(%s) = $2'%(table_name,geom),['int','text'])
plpy.notice('UPDATE %s SET km_lbl = $1 WHERE ST_AsText(%s) = $2'%(table_name,geom))
plpy.execute(plan, [km,geomCol])

return 'column created and table updated!!!!!'
$$
LANGUAGE plpython2u;

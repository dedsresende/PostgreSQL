--First step is to create a data type as a multi column table which the columns are the geometry column, the geometry indentification column and the shannon index column as float
CREATE TYPE shannon_table AS (geom GEOMETRY, geom_id TEXT, shannon_idx FLOAT);

--Function
CREATE OR REPLACE FUNCTION tm_shannon (points_table TEXT, points_geom TEXT, points_category TEXT, polygon_table TEXT, polygon_geom TEXT, polygon_id TEXT)

RETURNS SETOF shannon_table
AS 
$$

query = plpy.execute("""
WITH points AS(
SELECT 
%s AS pt_geom,
%s AS pt_cat
FROM %s
),

polys AS(
SELECT
%s AS poly_geom,
%s AS poly_id
FROM %s
),

step_0 AS(
SELECT
t.pt_geom,
t.pt_cat,
p.poly_id AS pid
FROM points t RIGHT JOIN polys p ON st_intersects(t.pt_geom, p.poly_geom)
),

step_1 AS(
SELECT
pid,
pt_cat,
(COUNT(1) OVER(PARTITION BY pid, pt_cat))::numeric / (COUNT(1) OVER(PARTITION BY pid))::numeric AS pi
FROM step_0
),

step_2 AS(
SELECT
pid,
pt_cat,
MAX(pi) AS pi
FROM step_1
GROUP BY pid, pt_cat
)

SELECT 
b.poly_geom as geom, 
a.pid,
-1*SUM(a.pi*(ln(a.pi))) AS sh
FROM step_2 a, polys b
WHERE a.pid = b.poly_id
GROUP BY a.pid, geom
"""%(points_geom,points_category,points_table,polygon_geom,polygon_id,polygon_table))

g = [x['geom'] for x in query]
id = [x['pid'] for x in query]
idx = [x['sh'] for x in query]

for g, i, x in zip(g,id,idx):
    yield (g, i, x)
$$
LANGUAGE plpythonu;

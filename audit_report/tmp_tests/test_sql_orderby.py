import duckdb
import pandas as pd

df = pd.read_csv('dashboard_test_data.csv')
con = duckdb.connect()
con.register('upload_test', df)

sql1 = '''SELECT strftime(DATE_TRUNC('month', TRY_CAST("date" AS DATE)), '%Y-%m') AS "date", SUM("revenue") AS "revenue"
FROM "upload_test"
GROUP BY strftime(DATE_TRUNC('month', TRY_CAST("date" AS DATE)), '%Y-%m')
ORDER BY "date" ASC'''

try:
    res = con.execute(sql1).df()
    print("ORDER BY 'date' worked! Rows:", len(res))
except Exception as e:
    print("ORDER BY 'date' error:", e)

sql2 = '''SELECT strftime(DATE_TRUNC('month', TRY_CAST("date" AS DATE)), '%Y-%m') AS "date", SUM("revenue") AS "revenue"
FROM "upload_test"
GROUP BY strftime(DATE_TRUNC('month', TRY_CAST("date" AS DATE)), '%Y-%m')
ORDER BY strftime(DATE_TRUNC('month', TRY_CAST("date" AS DATE)), '%Y-%m') ASC'''

try:
    res2 = con.execute(sql2).df()
    print("ORDER BY expr worked! Rows:", len(res2))
except Exception as e:
    print("ORDER BY expr error:", e)

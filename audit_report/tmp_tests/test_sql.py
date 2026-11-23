import duckdb
import pandas as pd

df = pd.read_csv('dashboard_test_data.csv')
con = duckdb.connect()
con.register('upload_test', df)

sql = '''SELECT strftime(DATE_TRUNC('month', TRY_CAST("date" AS DATE)), '%Y-%m') AS "date", SUM("revenue") AS "revenue"
FROM "upload_test"
GROUP BY strftime(DATE_TRUNC('month', TRY_CAST("date" AS DATE)), '%Y-%m')
ORDER BY 1 ASC'''

res = con.execute(sql).df()
print("Monthly rows:", len(res))
print("First date:", res.iloc[0]["date"])
print("Total revenue:", res["revenue"].sum())

sql_q = '''SELECT strftime(DATE_TRUNC('quarter', TRY_CAST("date" AS DATE)), '%Y') || '-Q' || QUARTER(TRY_CAST("date" AS DATE)) AS "date", SUM("revenue") AS "revenue"
FROM "upload_test"
GROUP BY strftime(DATE_TRUNC('quarter', TRY_CAST("date" AS DATE)), '%Y') || '-Q' || QUARTER(TRY_CAST("date" AS DATE))
ORDER BY 1 ASC'''

res_q = con.execute(sql_q).df()
print("Quarterly rows:", len(res_q))
print("First quarter:", res_q.iloc[0]["date"])

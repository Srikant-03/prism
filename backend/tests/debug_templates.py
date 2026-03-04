import pandas as pd, numpy as np
from sql.sql_engine import SQLEngine
from sql.template_generator import TemplateGenerator

engine = SQLEngine()
df = pd.DataFrame({
    "id": range(20),
    "name": [f"x{i}" for i in range(20)],
    "score": np.random.uniform(0, 100, 20),
    "group": np.random.choice(["A", "B"], 20),
})
engine.register_dataframe(df, "scores", "raw")

gen = TemplateGenerator(engine)
templates = gen.generate_templates("scores")

for t in templates:
    sql = t["sql"]
    for p in t["params"]:
        placeholder = "{{" + p["name"] + "}}"
        sql = sql.replace(placeholder, str(p["default"] if p["default"] is not None else 10))
    result = engine.execute(sql)
    if not result["success"]:
        print(f"FAIL: {t['title']}")
        print(f"  SQL: {sql}")
        print(f"  Error: {result['error']}")
    else:
        print(f"OK: {t['title']}")

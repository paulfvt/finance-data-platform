import pandas as pd
df = pd.read_parquet("data/gold/correlations_30d.parquet")
print(df)
print(len(df))
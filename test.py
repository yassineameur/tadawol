import pandas as pd

df = pd.DataFrame(data=[[1, 2], [10, 20], [100, 200]], columns=["1", "2"])

df["3"] = df["2"].shift(1)
print(df)

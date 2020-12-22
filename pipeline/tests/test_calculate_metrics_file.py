import pandas as pd
from pipeline.pipeline.calculate_metrics_file import calculate_metrics


if __name__ == '__main__':
    df = pd.read_csv("output/city_stats.csv", index_col=["date"])
    df.index = pd.to_datetime(df.index)
    df = df[df.district == "Mumbai"]
    calculate_metrics(df=df)
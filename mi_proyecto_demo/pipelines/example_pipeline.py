import polars as pl
from kipo.core.decorators import step
from kipo.core.definitions import DataLayer

# Simulating a pipeline
print("--- Starting Example Pipeline ---")

@step(layer=DataLayer.BRONZE, name="Load Data")
def load_data():
    # Simulate loading data
    df = pl.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})
    return df

@step(layer=DataLayer.SILVER)
def process_data(df: pl.DataFrame):
    return df.with_columns((pl.col("val") * 2).alias("val_x2"))

@step(layer=DataLayer.GOLD)
def final_step():
    print("Pipeline finished successfully")

if __name__ == "__main__":
    df = load_data()
    process_data(df)
    final_step()

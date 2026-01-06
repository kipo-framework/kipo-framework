import polars as pl
from kipo.core.decorators import step
from kipo.core.definitions import DataLayer
from kipo.core.io import read_raw  # <--- NUEVA IMPORTACIÓN

print("--- Starting Real World Pipeline ---")

@step(layer=DataLayer.BRONZE, name="ingest_cosechas") # Cambiamos nombre para reflejar realidad
def load_data():
    # AHORA LEEMOS EL ARCHIVO REAL DE LA CARPETA RAW
    return read_raw("cosecha_semanal.xlsx")

@step(layer=DataLayer.SILVER)
def process_data(df: pl.DataFrame):
    # Lógica de negocio real: Calcular proyección (ej. hectáreas * 10 tons/ha)
    return df.with_columns(
        (pl.col("hectareas") * 10).alias("toneladas_proyectadas")
    )

@step(layer=DataLayer.GOLD)
def final_report(df: pl.DataFrame):
    print("Resumen Ejecutivo:")
    print(df.select(["cultivo", "toneladas_proyectadas"]))
    return df

if __name__ == "__main__":
    df = load_data()
    processed = process_data(df) # Capturamos el retorno para pasarlo
    final_report(processed)
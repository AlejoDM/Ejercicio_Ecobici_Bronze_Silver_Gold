"""Lectura y escritura de Parquet compartidas por silver y gold."""

import os
from pathlib import Path

import pandas as pd


def escribir_parquet(df: pd.DataFrame, ruta: Path) -> None:
    """Escribe en un temporal y lo reemplaza: nunca queda un archivo a medias."""
    ruta.parent.mkdir(parents=True, exist_ok=True)
    temporal = ruta.with_name(ruta.name + ".tmp")
    df.to_parquet(temporal, index=False)
    os.replace(temporal, ruta)


def leer_particiones(carpeta: Path) -> pd.DataFrame:
    """Une las particiones anio=AAAA/data.parquet de una tabla, en orden de año."""
    archivos = sorted(carpeta.glob("anio=*/data.parquet"))
    return pd.concat([pd.read_parquet(archivo) for archivo in archivos], ignore_index=True)

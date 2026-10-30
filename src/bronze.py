"""Capa bronze: copia fiel de los archivos originales, en Parquet.

Todo se lee como texto y no se limpia nada. Cada año se reemplaza en su
propia carpeta, así que ejecutar dos veces no duplica datos.
"""

import os
import zipfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import ANIOS, ARCHIVOS_RECORRIDOS, ARCHIVOS_USUARIOS, BRONZE, RAW, TAMANIO_LOTE


@contextmanager
def _abrir_csv(ruta: Path):
    """Abre el CSV del archivo. Si es ZIP, toma el primer CSV interno."""
    if ruta.suffix == ".zip":
        with zipfile.ZipFile(ruta) as z:
            nombre = next(n for n in z.namelist() if n.lower().endswith(".csv"))
            with z.open(nombre) as f:
                yield f
    else:
        with open(ruta, "rb") as f:
            yield f


def _escribir_lotes(ruta: Path, temporal: Path, anio: int, fecha_ingesta: str) -> int:
    """Lee el CSV por lotes y escribe cada uno al Parquet temporal."""
    writer = None
    total = 0
    try:
        with _abrir_csv(ruta) as f:
            lotes = pd.read_csv(
                f,
                dtype=str,
                keep_default_na=False,
                na_values=[""],
                encoding="utf-8-sig",
                chunksize=TAMANIO_LOTE,
            )
            for lote in lotes:
                lote["_archivo_origen"] = ruta.name
                lote["_fecha_ingesta"] = fecha_ingesta
                lote["_anio"] = str(anio)
                # Esquema fijado con el primer lote: todas las columnas son texto.
                if writer is None:
                    esquema = pa.schema([(c, pa.string()) for c in lote.columns])
                    writer = pq.ParquetWriter(temporal, esquema)
                writer.write_table(pa.Table.from_pandas(lote, schema=esquema, preserve_index=False))
                total += len(lote)
    finally:
        if writer is not None:
            writer.close()
    return total


def ingestar_archivo(ruta: Path, anio: int, destino: Path) -> int:
    """Copia un archivo a bronze y devuelve la cantidad de filas escritas."""
    carpeta = destino / f"anio={anio}"
    carpeta.mkdir(parents=True, exist_ok=True)
    salida = carpeta / "data.parquet"
    # Se escribe a un temporal y se reemplaza al final: nunca queda un Parquet a medias.
    temporal = carpeta / "data.parquet.tmp"
    fecha_ingesta = datetime.now().isoformat(timespec="seconds")

    try:
        total = _escribir_lotes(ruta, temporal, anio, fecha_ingesta)
    except Exception:
        temporal.unlink(missing_ok=True)
        raise
    os.replace(temporal, salida)
    return total


def ingestar_recorridos() -> dict[int, int]:
    """Ingesta los recorridos de todos los años. Devuelve filas por año."""
    return {
        anio: ingestar_archivo(RAW / ARCHIVOS_RECORRIDOS[anio], anio, BRONZE / "recorridos")
        for anio in ANIOS
    }


def ingestar_usuarios() -> dict[int, int]:
    """Ingesta los usuarios de todos los años. Devuelve filas por año."""
    return {
        anio: ingestar_archivo(RAW / ARCHIVOS_USUARIOS[anio], anio, BRONZE / "usuarios")
        for anio in ANIOS
    }


def ejecutar_bronze() -> dict[str, dict[int, int]]:
    """Ejecuta la capa bronze completa y devuelve filas escritas por tabla y año."""
    return {"recorridos": ingestar_recorridos(), "usuarios": ingestar_usuarios()}

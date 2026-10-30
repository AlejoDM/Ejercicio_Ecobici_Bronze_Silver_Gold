"""Exploración de los archivos originales (Tarea 1).

No forma parte del pipeline: sirve para completar la tabla de DECISIONES.md.
Ejecutar con: python -m src.exploracion
"""

import zipfile
from pathlib import Path

import pandas as pd

from src.config import ANIOS, ARCHIVOS_RECORRIDOS, ARCHIVOS_USUARIOS, RAW, TAMANIO_LOTE

FILAS_MUESTRA = 1000


def _nombre_interno(ruta: Path) -> str | None:
    """Nombre del CSV dentro del ZIP, o None si el archivo no es ZIP."""
    if ruta.suffix != ".zip":
        return None
    with zipfile.ZipFile(ruta) as z:
        return z.namelist()[0]


def _separador(ruta: Path) -> str:
    """Detecta ',' o ';' mirando la primera línea del archivo."""
    nombre = _nombre_interno(ruta)
    if nombre:
        with zipfile.ZipFile(ruta) as z, z.open(nombre) as f:
            cabecera = f.readline().decode("utf-8-sig")
    else:
        with open(ruta, encoding="utf-8-sig") as f:
            cabecera = f.readline()
    return ";" if cabecera.count(";") > cabecera.count(",") else ","


def _opciones_lectura(ruta: Path) -> dict:
    """Parámetros comunes de lectura: todo como texto, con BOM tolerado."""
    return {
        "compression": "zip" if ruta.suffix == ".zip" else None,
        "sep": _separador(ruta),
        "dtype": str,
        "encoding": "utf-8-sig",
    }


def explorar(ruta: Path) -> dict:
    """Describe un archivo sin cargarlo completo."""
    opciones = _opciones_lectura(ruta)
    muestra = pd.read_csv(ruta, nrows=FILAS_MUESTRA, **opciones)
    # Contar filas por lotes, leyendo solo la primera columna.
    filas = sum(
        len(lote)
        for lote in pd.read_csv(ruta, usecols=[0], chunksize=TAMANIO_LOTE, **opciones)
    )
    return {
        "archivo": ruta.name,
        "dentro_del_zip": _nombre_interno(ruta),
        "separador": opciones["sep"],
        "columnas": list(muestra.columns),
        "filas": filas,
        "muestra": muestra.head(2),
    }


def main() -> None:
    for anio in ANIOS:
        for nombre in (ARCHIVOS_RECORRIDOS[anio], ARCHIVOS_USUARIOS[anio]):
            resumen = explorar(RAW / nombre)
            print(f"\n=== {anio} | {resumen['archivo']} ===")
            print(f"Dentro del ZIP: {resumen['dentro_del_zip']}")
            print(f"Separador: {resumen['separador']!r}")
            print(f"Filas: {resumen['filas']:,}")
            print(f"Columnas: {resumen['columnas']}")
            print(resumen["muestra"].to_string())


if __name__ == "__main__":
    main()

"""Ejecuta el pipeline completo, de bronze a los reportes.

Uso: python main.py
"""

import time
from collections.abc import Callable
from typing import Any

from src.bronze import ejecutar_bronze
from src.config import ANIOS, ARCHIVOS_RECORRIDOS, ARCHIVOS_USUARIOS, RAW


def verificar_archivos_originales() -> None:
    """Falla antes de empezar si falta algún archivo descargado."""
    esperados = [ARCHIVOS_RECORRIDOS[a] for a in ANIOS] + [ARCHIVOS_USUARIOS[a] for a in ANIOS]
    faltantes = [nombre for nombre in esperados if not (RAW / nombre).exists()]
    if faltantes:
        lista = "\n  - ".join(faltantes)
        raise FileNotFoundError(f"Faltan archivos en data/raw/:\n  - {lista}")


def ejecutar_paso(nombre: str, funcion: Callable[[], Any]) -> Any:
    """Corre un paso del pipeline e informa cuánto tardó."""
    print(f"\n>> {nombre}")
    inicio = time.perf_counter()
    resultado = funcion()
    print(f"   listo en {time.perf_counter() - inicio:.1f} s")
    return resultado


def main() -> None:
    verificar_archivos_originales()

    filas_bronze = ejecutar_paso("Bronze", ejecutar_bronze)
    for tabla, por_anio in filas_bronze.items():
        for anio, cantidad in por_anio.items():
            print(f"   {tabla} {anio}: {cantidad:,} filas")


if __name__ == "__main__":
    main()

"""Ejecuta el pipeline completo, de bronze a los reportes.

Uso: python main.py
"""

import sys
import time
from collections.abc import Callable
from typing import Any

from rich.progress import Progress

from src.bronze import ejecutar_bronze
from src.config import ANIOS, ARCHIVOS_RECORRIDOS, ARCHIVOS_USUARIOS, RAW
from src.gold import ejecutar_gold
from src.progreso import crear_progreso
from src.reportes import ejecutar_reportes
from src.silver import ejecutar_silver


def verificar_archivos_originales() -> None:
    """Falla antes de empezar si falta algún archivo descargado."""
    esperados = [ARCHIVOS_RECORRIDOS[a] for a in ANIOS] + [ARCHIVOS_USUARIOS[a] for a in ANIOS]
    faltantes = [nombre for nombre in esperados if not (RAW / nombre).exists()]
    if faltantes:
        lista = "\n  - ".join(faltantes)
        raise FileNotFoundError(f"Faltan archivos en data/raw/:\n  - {lista}")


def ejecutar_paso(progreso: Progress, nombre: str, funcion: Callable[[], Any]) -> Any:
    """Corre un paso con una barra que muestra el tiempo transcurrido."""
    tarea = progreso.add_task(nombre, total=None)
    inicio = time.perf_counter()
    resultado = funcion()
    progreso.update(tarea, total=1, completed=1)
    progreso.console.print(f"[green]{nombre} listo[/] en {time.perf_counter() - inicio:.1f} s")
    return resultado


def main() -> None:
    verificar_archivos_originales()
    inicio_total = time.perf_counter()

    with crear_progreso() as progreso:
        filas_bronze = ejecutar_paso(progreso, "Bronze", lambda: ejecutar_bronze(progreso))
        for tabla, por_anio in filas_bronze.items():
            for anio, cantidad in por_anio.items():
                progreso.console.print(f"   {tabla} {anio}: {cantidad:,} filas")

        resumen_silver = ejecutar_paso(progreso, "Silver", ejecutar_silver)
        for tabla, por_anio in resumen_silver.items():
            for anio, (validos, cuarentena) in por_anio.items():
                progreso.console.print(
                    f"   {tabla} {anio}: {validos:,} válidos, {cuarentena:,} en cuarentena"
                )

        sin_usuario = ejecutar_paso(progreso, "Gold", ejecutar_gold)
        for motivo, cantidad in sin_usuario.items():
            progreso.console.print(f"   viajes {motivo}: {cantidad:,}")

        reportes = ejecutar_paso(progreso, "Reportes", ejecutar_reportes)
        for ruta in reportes.values():
            progreso.console.print(f"   {ruta.name}")

    minutos, segundos = divmod(time.perf_counter() - inicio_total, 60)
    print(f"\nPipeline completo en {int(minutos)} min {segundos:.1f} s")


if __name__ == "__main__":
    # Las barras usan caracteres Unicode, que las consolas de Windows con cp1252 no pueden mostrar.
    sys.stdout.reconfigure(encoding="utf-8")
    main()

"""Reportes en CSV para el negocio, generados desde las tablas gold."""

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.config import ANIOS, BRONZE, GOLD, REPORTES, SILVER

TOP_ESTACIONES = 10
DESCONOCIDO = "DESCONOCIDO"


def _leer_gold(nombre: str) -> pd.DataFrame:
    return pd.read_parquet(GOLD / f"{nombre}.parquet")


def _filas_parquet(ruta: Path) -> int:
    """Cantidad de filas de un Parquet, leída de sus metadatos."""
    return pq.ParquetFile(ruta).metadata.num_rows


def reporte_viajes_por_mes(viajes: pd.DataFrame) -> pd.DataFrame:
    """Cantidad de viajes por año y mes."""
    return viajes.groupby(["anio", "mes"]).size().reset_index(name="cantidad_viajes")


def reporte_top_estaciones(viajes: pd.DataFrame, dim_estacion: pd.DataFrame) -> pd.DataFrame:
    """Las diez estaciones con más viajes iniciados en cada año."""
    conteo = viajes.groupby(["anio", "id_estacion_origen"]).size().reset_index(name="cantidad_viajes")
    # Ante empates, se ordena por id de estación para que el resultado sea siempre el mismo.
    conteo = conteo.sort_values(
        ["anio", "cantidad_viajes", "id_estacion_origen"], ascending=[True, False, True]
    ).groupby("anio").head(TOP_ESTACIONES)
    conteo["ranking"] = conteo.groupby("anio").cumcount() + 1

    conteo = conteo.merge(
        dim_estacion[["id_estacion", "nombre_estacion"]],
        left_on="id_estacion_origen",
        right_on="id_estacion",
        how="left",
    )
    return conteo[["anio", "ranking", "nombre_estacion", "cantidad_viajes"]]


def reporte_duracion_por_rango_y_genero(viajes: pd.DataFrame, dim_usuario: pd.DataFrame) -> pd.DataFrame:
    """Duración promedio según rango etario y género del usuario."""
    datos = viajes[["id_usuario", "duracion_min"]].merge(
        dim_usuario[["id_usuario", "rango_etario", "genero"]], on="id_usuario", how="left"
    )
    # Los viajes sin usuario conocido se agrupan como DESCONOCIDO.
    datos[["rango_etario", "genero"]] = datos[["rango_etario", "genero"]].fillna(DESCONOCIDO)

    resumen = datos.groupby(["rango_etario", "genero"]).agg(
        cantidad_viajes=("duracion_min", "size"),
        duracion_promedio_min=("duracion_min", "mean"),
    )
    resumen["duracion_promedio_min"] = resumen.duracion_promedio_min.round(1)
    return resumen.reset_index()


def reporte_viajes_circulares(viajes: pd.DataFrame) -> pd.DataFrame:
    """Porcentaje de viajes que terminan en la misma estación donde empezaron, por año."""
    resumen = viajes.groupby("anio").agg(
        total_viajes=("es_circular", "size"),
        viajes_circulares=("es_circular", "sum"),
    ).reset_index()
    resumen["porcentaje_circulares"] = (100 * resumen.viajes_circulares / resumen.total_viajes).round(1)
    return resumen


def reporte_calidad() -> pd.DataFrame:
    """Filas de bronze, silver y cuarentena por año de origen.

    Es la única excepción a usar solo gold: necesita contar en cada capa.
    """
    filas = []
    for anio in ANIOS:
        bronze = _filas_parquet(BRONZE / "recorridos" / f"anio={anio}" / "data.parquet")
        silver = _filas_parquet(SILVER / "viajes" / f"anio={anio}" / "data.parquet")
        cuarentena = _filas_parquet(SILVER / "viajes_cuarentena" / f"anio={anio}" / "data.parquet")
        filas.append(
            {
                "anio": anio,
                "filas_bronze": bronze,
                "filas_silver": silver,
                "filas_cuarentena": cuarentena,
                "porcentaje_cuarentena": round(100 * cuarentena / bronze, 2),
            }
        )
    return pd.DataFrame(filas)


def ejecutar_reportes() -> dict[str, Path]:
    """Genera los cinco reportes y devuelve la ruta de cada CSV."""
    REPORTES.mkdir(parents=True, exist_ok=True)
    fact = _leer_gold("fact_viajes")
    dim_fecha = _leer_gold("dim_fecha")
    dim_estacion = _leer_gold("dim_estacion")
    dim_usuario = _leer_gold("dim_usuario")

    viajes = fact.merge(dim_fecha[["fecha_key", "anio", "mes"]], on="fecha_key")
    reportes = {
        "viajes_por_mes.csv": reporte_viajes_por_mes(viajes),
        "top10_estaciones_por_anio.csv": reporte_top_estaciones(viajes, dim_estacion),
        "duracion_por_rango_y_genero.csv": reporte_duracion_por_rango_y_genero(viajes, dim_usuario),
        "viajes_circulares.csv": reporte_viajes_circulares(viajes),
        "calidad_por_anio.csv": reporte_calidad(),
    }

    rutas = {}
    for nombre, tabla in reportes.items():
        rutas[nombre] = REPORTES / nombre
        tabla.to_csv(rutas[nombre], index=False)
    return rutas

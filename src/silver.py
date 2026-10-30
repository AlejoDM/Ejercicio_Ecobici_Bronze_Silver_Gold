"""Capa silver: datos limpios, unificados y validados.

Cada año se lleva al mismo esquema canónico. Los registros que no pasan
los controles de calidad se mueven a una tabla de cuarentena con su motivo.
"""

import pandas as pd
import pyarrow.parquet as pq

from src.config import BRONZE

# Columnas originales de cada año hacia el esquema canónico.
# Las que un año no trae no aparecen acá: se completan al transformar.
MAPEO_VIAJES = {
    2016: {
        "fecha_origen_recorrido": "fecha_hora_inicio",
        "fecha_destino_recorrido": "fecha_hora_fin",
        "id_estacion_origen": "id_estacion_origen",
        "nombre_estacion_origen": "nombre_estacion_origen",
        "id_estacion_destino": "id_estacion_destino",
        "nombre_estacion_destino": "nombre_estacion_destino",
    },
    2019: {
        "Id_recorrido": "id_viaje",
        "fecha_origen_recorrido": "fecha_hora_inicio",
        "fecha_destino_recorrido": "fecha_hora_fin",
        "duracion_recorrido": "duracion_seg",
        "id_estacion_origen": "id_estacion_origen",
        "nombre_estacion_origen": "nombre_estacion_origen",
        "id_estacion_destino": "id_estacion_destino",
        "nombre_estacion_destino": "nombre_estacion_destino",
        "id_usuario": "id_usuario",
    },
    2024: {
        "id_recorrido": "id_viaje",
        "fecha_origen_recorrido": "fecha_hora_inicio",
        "fecha_destino_recorrido": "fecha_hora_fin",
        "duracion_recorrido": "duracion_seg",
        "id_estacion_origen": "id_estacion_origen",
        "nombre_estacion_origen": "nombre_estacion_origen",
        "id_estacion_destino": "id_estacion_destino",
        "nombre_estacion_destino": "nombre_estacion_destino",
        "id_usuario": "id_usuario",
    },
}

MAPEO_USUARIOS = {
    2016: {
        "id_usuario": "id_usuario",
        "genero_usuario": "genero",
        "edad_usuario": "edad",
        "fecha_alta": "fecha_alta_texto",
        "hora_alta": "hora_alta_texto",
    },
    2019: {
        "ID_usuario": "id_usuario",
        "genero_usuario": "genero",
        "edad_usuario": "edad",
        "fecha_alta": "fecha_alta_texto",
        "hora_alta": "hora_alta_texto",
    },
    2024: {
        "id_usuario": "id_usuario",
        "genero_usuario": "genero",
        "edad_usuario": "edad",
        "fecha_alta": "fecha_alta_texto",
        "hora_alta": "hora_alta_texto",
    },
}

# Formatos de fecha. Los de viajes son iguales en los tres años.
FORMATO_FECHA_VIAJE = "%Y-%m-%d %H:%M:%S"
FORMATO_FECHA_ALTA = {
    2016: "%d-%m-%y %I:%M:%S %p",  # p. ej. 01-01-16 3:45:19 AM
    2019: "%Y-%m-%d %H:%M:%S",
    2024: "%Y-%m-%d %H:%M:%S",
}


def _leer_bronze(tabla: str, anio: int, mapeo: dict[str, str]) -> pd.DataFrame:
    """Lee de bronze solo las columnas que están en el mapeo."""
    ruta = BRONZE / tabla / f"anio={anio}" / "data.parquet"
    return pq.read_table(ruta, columns=list(mapeo)).to_pandas()


def renombrar(df: pd.DataFrame, mapeo: dict[str, str]) -> pd.DataFrame:
    """Renombra al esquema canónico y descarta lo que no está en el mapeo."""
    return df.rename(columns=mapeo)[list(mapeo.values())]

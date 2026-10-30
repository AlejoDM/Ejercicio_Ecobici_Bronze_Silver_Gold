"""Capa silver: datos limpios, unificados y validados.

Cada año se lleva al mismo esquema canónico. Los registros que no pasan
los controles de calidad se mueven a una tabla de cuarentena con su motivo.
"""

import re
import unicodedata

import pandas as pd
import pyarrow.parquet as pq

from src.config import BRONZE, EDAD_MAXIMA, EDAD_MINIMA

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

# Esquema canónico: columnas y tipos que tienen que tener todas las tablas silver.
ESQUEMA_VIAJES = {
    "id_viaje": "string",
    "fecha_hora_inicio": "datetime64[ns]",
    "fecha_hora_fin": "datetime64[ns]",
    "duracion_seg": "Int64",
    "id_estacion_origen": "Int64",
    "nombre_estacion_origen": "string",
    "id_estacion_destino": "Int64",
    "nombre_estacion_destino": "string",
    "id_usuario": "string",
    "anio_fuente": "Int64",
}

ESQUEMA_USUARIOS = {
    "id_usuario": "string",
    "genero": "string",
    "edad": "Int64",
    "fecha_alta": "datetime64[ns]",
    "anio_fuente": "Int64",
}

# Valores de género de las tres fuentes, llevados a cuatro categorías.
GENEROS = {
    "F": "F",
    "FEMALE": "F",
    "FEMENINO": "F",
    "M": "M",
    "MALE": "M",
    "MASCULINO": "M",
    "OTHER": "OTRO",
    "OTRO": "OTRO",
    "X": "OTRO",
}


def _leer_bronze(tabla: str, anio: int, mapeo: dict[str, str]) -> pd.DataFrame:
    """Lee de bronze solo las columnas que están en el mapeo."""
    ruta = BRONZE / tabla / f"anio={anio}" / "data.parquet"
    return pq.read_table(ruta, columns=list(mapeo)).to_pandas()


def renombrar(df: pd.DataFrame, mapeo: dict[str, str]) -> pd.DataFrame:
    """Renombra al esquema canónico y descarta lo que no está en el mapeo."""
    return df.rename(columns=mapeo)[list(mapeo.values())]


def _id_texto(serie: pd.Series) -> pd.Series:
    """Quita el sufijo 'BAEcobici' y el '.0' de los ids, y los deja como texto."""
    limpio = serie.str.strip().str.replace("BAEcobici", "", regex=False)
    return limpio.str.replace(r"\.0$", "", regex=True).astype("string")


def _id_entero(serie: pd.Series) -> pd.Series:
    """Id de estación como entero. Lo que no es numérico queda nulo."""
    return pd.to_numeric(_id_texto(serie), errors="coerce").astype("Int64")


def _fecha(serie: pd.Series, formato: str) -> pd.Series:
    """Convierte a fecha con el formato del año. Lo que no se entiende queda NaT."""
    return pd.to_datetime(serie, format=formato, errors="coerce").astype("datetime64[ns]")


def _normalizar_nombre(texto: str) -> str | None:
    """Sin tildes, en mayúsculas, con espacios simples y sin el código inicial ('005 - ')."""
    sin_tildes = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    limpio = re.sub(r"\s+", " ", sin_tildes.strip().upper())
    limpio = re.sub(r"^\d+\s*-\s*", "", limpio)
    return limpio or None


def _nombres_estacion(serie: pd.Series) -> pd.Series:
    """Normaliza los nombres de estación evaluando cada valor distinto una sola vez."""
    equivalencias = {v: _normalizar_nombre(v) for v in serie.dropna().unique()}
    return serie.map(equivalencias).astype("string")


def _genero(serie: pd.Series) -> pd.Series:
    """Género en F, M, OTRO o DESCONOCIDO (vacío o valor no reconocido)."""
    return serie.str.strip().str.upper().map(GENEROS).fillna("DESCONOCIDO").astype("string")


def transformar_usuarios(anio: int) -> pd.DataFrame:
    """Lleva los usuarios de un año al esquema canónico."""
    mapeo = MAPEO_USUARIOS[anio]
    df = renombrar(_leer_bronze("usuarios", anio, mapeo), mapeo)

    # Fecha y hora vienen en columnas separadas: se unen antes de convertir.
    alta = df.pop("fecha_alta_texto") + " " + df.pop("hora_alta_texto")
    df["fecha_alta"] = _fecha(alta, FORMATO_FECHA_ALTA[anio])
    df["id_usuario"] = _id_texto(df.id_usuario)
    df["genero"] = _genero(df.genero)

    # Las edades fuera de rango no se rechazan: quedan nulas.
    edad = pd.to_numeric(df.edad, errors="coerce")
    df["edad"] = edad.where(edad.between(EDAD_MINIMA, EDAD_MAXIMA)).astype("Int64")

    df["anio_fuente"] = anio
    return df.astype(ESQUEMA_USUARIOS)[list(ESQUEMA_USUARIOS)]


def transformar_viajes(anio: int) -> pd.DataFrame:
    """Lleva los viajes de un año al esquema canónico, con tipos correctos."""
    mapeo = MAPEO_VIAJES[anio]
    df = renombrar(_leer_bronze("recorridos", anio, mapeo), mapeo)

    df["fecha_hora_inicio"] = _fecha(df.fecha_hora_inicio, FORMATO_FECHA_VIAJE)
    df["fecha_hora_fin"] = _fecha(df.fecha_hora_fin, FORMATO_FECHA_VIAJE)
    df["id_estacion_origen"] = _id_entero(df.id_estacion_origen)
    df["id_estacion_destino"] = _id_entero(df.id_estacion_destino)
    df["nombre_estacion_origen"] = _nombres_estacion(df.nombre_estacion_origen)
    df["nombre_estacion_destino"] = _nombres_estacion(df.nombre_estacion_destino)

    if "duracion_seg" in df:
        # Los años con duración en segundos traen separador de miles ("2,814").
        df["duracion_seg"] = pd.to_numeric(
            df.duracion_seg.str.replace(",", "", regex=False), errors="coerce"
        ).astype("Int64")
    else:
        # 2016: la duración declarada no coincide con las fechas; se calcula.
        df["duracion_seg"] = (df.fecha_hora_fin - df.fecha_hora_inicio).dt.total_seconds()
        df["duracion_seg"] = df.duracion_seg.round().astype("Int64")

    if "id_viaje" in df:
        df["id_viaje"] = f"{anio}-" + _id_texto(df.id_viaje)
    else:
        # 2016 no trae id de viaje: se genera según el orden en bronze.
        df["id_viaje"] = [f"{anio}-{n:07d}" for n in range(1, len(df) + 1)]

    if "id_usuario" in df:
        df["id_usuario"] = _id_texto(df.id_usuario)
    else:
        # 2016 no trae id de usuario.
        df["id_usuario"] = pd.NA

    df["anio_fuente"] = anio
    return df.astype(ESQUEMA_VIAJES)[list(ESQUEMA_VIAJES)]

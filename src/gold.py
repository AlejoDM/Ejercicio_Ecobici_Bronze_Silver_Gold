"""Capa gold: modelo estrella listo para el negocio.

Gold solo arma tablas a partir de silver: no limpia ni corrige nada.
"""

import pandas as pd

from src.almacenamiento import escribir_parquet, leer_particiones
from src.config import GOLD, SILVER

DIAS_SEMANA = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]

RANGOS_ETARIOS = [0, 25, 35, 50, float("inf")]
ETIQUETAS_ETARIAS = ["16-25", "26-35", "36-50", "51+"]


def _clave_fecha(fecha: pd.Series) -> pd.Series:
    """Fecha como entero AAAAMMDD, la clave de dim_fecha."""
    return fecha.dt.strftime("%Y%m%d").astype("int64")


def _rango_etario(edad: pd.Series) -> pd.Series:
    """Rango etario del usuario; sin edad declarada queda DESCONOCIDO."""
    rangos = pd.cut(edad.astype("float64"), bins=RANGOS_ETARIOS, labels=ETIQUETAS_ETARIAS)
    return rangos.astype("string").fillna("DESCONOCIDO")


def construir_dim_fecha(viajes: pd.DataFrame) -> pd.DataFrame:
    """Un día por cada fecha entre el primer y el último viaje, sin huecos."""
    inicio = viajes.fecha_hora_inicio.min().normalize()
    fin = viajes.fecha_hora_inicio.max().normalize()

    dim = pd.DataFrame({"fecha": pd.date_range(inicio, fin, freq="D")})
    dim["fecha_key"] = _clave_fecha(dim.fecha)
    dim["anio"] = dim.fecha.dt.year.astype("int64")
    dim["mes"] = dim.fecha.dt.month.astype("int64")
    dim["dia"] = dim.fecha.dt.day.astype("int64")
    # Nombre en español fijo, sin depender del idioma configurado en el sistema.
    dim["dia_semana"] = dim.fecha.dt.dayofweek.map(dict(enumerate(DIAS_SEMANA)))
    dim["es_fin_de_semana"] = dim.fecha.dt.dayofweek >= 5
    return dim[["fecha_key", "fecha", "anio", "mes", "dia", "dia_semana", "es_fin_de_semana"]]


def construir_dim_estacion(viajes: pd.DataFrame) -> pd.DataFrame:
    """Una fila por estación, con el nombre del año más reciente en que aparece."""
    apariciones = pd.concat(
        [
            viajes[["id_estacion_origen", "nombre_estacion_origen", "anio_fuente"]].set_axis(
                ["id_estacion", "nombre_estacion", "anio"], axis=1
            ),
            viajes[["id_estacion_destino", "nombre_estacion_destino", "anio_fuente"]].set_axis(
                ["id_estacion", "nombre_estacion", "anio"], axis=1
            ),
        ],
        ignore_index=True,
    ).dropna(subset=["id_estacion"])

    rangos = apariciones.groupby("id_estacion").agg(
        primer_anio=("anio", "min"), ultimo_anio=("anio", "max")
    )

    # Del último año de cada estación, el nombre más frecuente.
    del_ultimo_anio = apariciones[apariciones.anio == apariciones.id_estacion.map(rangos.ultimo_anio)]
    nombres = (
        del_ultimo_anio.dropna(subset=["nombre_estacion"])
        .groupby(["id_estacion", "nombre_estacion"])
        .size()
        .reset_index(name="cantidad")
        .sort_values(["id_estacion", "cantidad"], ascending=[True, False])
        .drop_duplicates("id_estacion")
        .set_index("id_estacion")["nombre_estacion"]
    )

    dim = rangos.join(nombres).reset_index()
    return dim[["id_estacion", "nombre_estacion", "primer_anio", "ultimo_anio"]]


def construir_dim_usuario(usuarios: pd.DataFrame) -> pd.DataFrame:
    """Una fila por usuario, con rango etario y año de alta."""
    return pd.DataFrame(
        {
            "id_usuario": usuarios.id_usuario,
            "genero": usuarios.genero,
            "edad": usuarios.edad,
            "rango_etario": _rango_etario(usuarios.edad),
            "anio_alta": usuarios.fecha_alta.dt.year.astype("int64"),
        }
    )


def construir_fact_viajes(viajes: pd.DataFrame) -> pd.DataFrame:
    """Una fila por viaje válido, con claves hacia las dimensiones."""
    return pd.DataFrame(
        {
            "id_viaje": viajes.id_viaje,
            "fecha_key": _clave_fecha(viajes.fecha_hora_inicio),
            "hora_inicio": viajes.fecha_hora_inicio.dt.hour.astype("int64"),
            "id_estacion_origen": viajes.id_estacion_origen,
            "id_estacion_destino": viajes.id_estacion_destino,
            "id_usuario": viajes.id_usuario,
            "duracion_min": (viajes.duracion_seg / 60).round(1).astype("float64"),
            "es_circular": (viajes.id_estacion_origen == viajes.id_estacion_destino).astype(bool),
        }
    )


def verificar_modelo(
    fact: pd.DataFrame,
    viajes: pd.DataFrame,
    dim_fecha: pd.DataFrame,
    dim_estacion: pd.DataFrame,
    dim_usuario: pd.DataFrame,
) -> dict[str, int]:
    """Falla si el modelo está roto. Devuelve cuántos viajes no tienen usuario conocido."""
    if len(fact) != len(viajes):
        raise RuntimeError(f"fact_viajes tiene {len(fact):,} filas y silver {len(viajes):,}")
    if not fact.fecha_key.isin(dim_fecha.fecha_key).all():
        raise RuntimeError("Hay fecha_key en fact_viajes que no existe en dim_fecha")

    estaciones = dim_estacion.id_estacion
    if not (fact.id_estacion_origen.isin(estaciones).all() and fact.id_estacion_destino.isin(estaciones).all()):
        raise RuntimeError("Hay estaciones en fact_viajes que no existen en dim_estacion")

    # No es un error: el id de usuario de los viajes no siempre coincide con el archivo de usuarios.
    con_id = fact.id_usuario.notna()
    return {
        "sin_id_usuario": int((~con_id).sum()),
        "id_usuario_no_encontrado": int((con_id & ~fact.id_usuario.isin(dim_usuario.id_usuario)).sum()),
    }


def ejecutar_gold() -> dict[str, int]:
    """Construye el modelo estrella desde silver y lo guarda en data/gold/."""
    viajes = leer_particiones(SILVER / "viajes")
    usuarios = leer_particiones(SILVER / "usuarios")

    dim_fecha = construir_dim_fecha(viajes)
    dim_estacion = construir_dim_estacion(viajes)
    dim_usuario = construir_dim_usuario(usuarios)
    fact = construir_fact_viajes(viajes)
    sin_usuario = verificar_modelo(fact, viajes, dim_fecha, dim_estacion, dim_usuario)

    escribir_parquet(dim_fecha, GOLD / "dim_fecha.parquet")
    escribir_parquet(dim_estacion, GOLD / "dim_estacion.parquet")
    escribir_parquet(dim_usuario, GOLD / "dim_usuario.parquet")
    escribir_parquet(fact, GOLD / "fact_viajes.parquet")
    return sin_usuario

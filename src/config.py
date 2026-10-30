"""Rutas, años y constantes del pipeline."""

from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent

RAW = RAIZ / "data" / "raw"
BRONZE = RAIZ / "data" / "bronze"
SILVER = RAIZ / "data" / "silver"
GOLD = RAIZ / "data" / "gold"
REPORTES = GOLD / "reportes"

ANIOS = (2016, 2019, 2024)

ARCHIVOS_RECORRIDOS = {
    2016: "recorridos-realizados-2016.zip",
    2019: "recorridos-realizados-2019.zip",
    2024: "recorridos-realizados-2024.zip",
}

ARCHIVOS_USUARIOS = {
    2016: "usuarios-ecobici-2016.csv",
    2019: "usuarios_ecobici_2019.csv",
    2024: "usuarios_ecobici_2024.csv",
}

# Filas por lote al leer los CSV grandes (evita cargar todo en memoria).
TAMANIO_LOTE = 500_000

# Reglas de calidad de silver.
DURACION_MAXIMA_SEG = 86_400
EDAD_MINIMA = 16
EDAD_MAXIMA = 90

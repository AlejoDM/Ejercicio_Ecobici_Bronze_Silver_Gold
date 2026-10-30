# Pipeline Ecobici: Bronze, Silver y Gold

Pipeline de datos sobre los viajes y usuarios del sistema de bicicletas públicas Ecobici (Ciudad de Buenos Aires) de los años 2016, 2019 y 2024. Toma los archivos tal como los publica la fuente y genera tablas listas para responder preguntas de negocio, organizadas en tres capas:

- **Bronze:** copia fiel de los archivos originales, en Parquet y sin limpiar.
- **Silver:** datos unificados en un esquema único, con tipos correctos y controles de calidad. Los registros rechazados van a una tabla de cuarentena con su motivo.
- **Gold:** modelo estrella (una tabla de hechos y tres dimensiones) y los cinco reportes en CSV.

Las decisiones que no están en el enunciado están documentadas en [DECISIONES.md](DECISIONES.md).

## Requisitos

- Python 3.14 (versión con la que se probó el proyecto).
- Unos 4 GB de espacio libre para los datos generados, y unos 2 GB de memoria RAM disponibles.

## Instalación

```bash
python -m venv .venv
```

Activar el entorno virtual:

- Windows: `.venv\Scripts\activate`
- Linux o macOS: `source .venv/bin/activate`

Instalar las dependencias:

```bash
pip install -r requirements.txt
```

## Datos de entrada

Descargar desde el [portal de datos abiertos de la Ciudad de Buenos Aires](https://data.buenosaires.gob.ar/dataset/bicicletas-publicas) y copiar **sin modificar** en `data/raw/` los seis archivos siguientes:

| Archivo | Descripción |
|---|---|
| `recorridos-realizados-2016.zip` | Viajes de 2016 (ZIP) |
| `recorridos-realizados-2019.zip` | Viajes de 2019 (ZIP) |
| `recorridos-realizados-2024.zip` | Viajes de 2024 (ZIP) |
| `usuarios-ecobici-2016.csv` | Usuarios de 2016 |
| `usuarios_ecobici_2019.csv` | Usuarios de 2019 |
| `usuarios_ecobici_2024.csv` | Usuarios de 2024 |

Los archivos no se descomprimen a mano: el pipeline lee cada ZIP directamente.

## Ejecución

```bash
python main.py
```

Ejecuta el pipeline completo, de bronze a los reportes, sin pasos manuales. Antes de empezar verifica que estén los seis archivos de `data/raw/`. En la máquina de prueba tardó unos 3 minutos.

Es idempotente: ejecutarlo varias veces produce los mismos datos y reportes, sin duplicar filas.

## Salidas

| Carpeta | Contenido |
|---|---|
| `data/bronze/recorridos/anio=AAAA/` y `data/bronze/usuarios/anio=AAAA/` | Datos originales en Parquet, con columnas de metadatos de ingesta |
| `data/silver/viajes/` y `data/silver/usuarios/` | Tablas válidas, particionadas por año |
| `data/silver/viajes_cuarentena/` y `data/silver/usuarios_cuarentena/` | Registros rechazados, con la columna `motivo` |
| `data/gold/` | `dim_fecha`, `dim_estacion`, `dim_usuario` y `fact_viajes` en Parquet |
| `data/gold/reportes/` | Los cinco reportes en CSV |

Las carpetas `raw`, `bronze`, `silver` y las tablas de `gold` no se versionan: se regeneran con el pipeline. Los reportes sí.

## Reportes

| Archivo | Contenido |
|---|---|
| `viajes_por_mes.csv` | Cantidad de viajes por año y mes |
| `top10_estaciones_por_anio.csv` | Las diez estaciones con más viajes iniciados en cada año |
| `duracion_por_rango_y_genero.csv` | Duración promedio según rango etario y género del usuario |
| `viajes_circulares.csv` | Porcentaje de viajes que terminan en la estación donde empezaron |
| `calidad_por_anio.csv` | Filas de bronze, silver y cuarentena por año de origen |

## Verificaciones

El pipeline se detiene si alguna de estas comprobaciones falla:

- **Conciliación:** en cada año, las filas de bronze son iguales a las de silver más las de cuarentena.
- **Esquema:** columnas y tipos de cada tabla silver coinciden exactamente con el esquema canónico.
- **Unicidad:** `id_viaje` es único en silver y `id_usuario` es único en silver.
- **Integridad del modelo:** `fact_viajes` tiene las mismas filas que silver, y todas sus claves de fecha y estación existen en las dimensiones.

## Estructura del proyecto

```
ecobici-pipeline/
├── data/
│   ├── raw/          # archivos descargados, sin modificar (no versionado)
│   ├── bronze/       # copia fiel en Parquet (no versionado)
│   ├── silver/       # datos limpios y cuarentena (no versionado)
│   └── gold/
│       └── reportes/ # reportes CSV
├── src/
│   ├── config.py       # rutas, años y constantes
│   ├── almacenamiento.py # escritura y lectura de Parquet
│   ├── exploracion.py  # relevamiento de los archivos originales (Tarea 1)
│   ├── bronze.py       # capa bronze
│   ├── silver.py       # capa silver
│   ├── gold.py         # capa gold
│   └── reportes.py     # reportes en CSV
├── main.py           # ejecuta el pipeline completo
├── requirements.txt
├── DECISIONES.md
└── README.md
```

`src/exploracion.py` no forma parte del pipeline. Para volver a ver el relevamiento de los archivos: `python -m src.exploracion`.

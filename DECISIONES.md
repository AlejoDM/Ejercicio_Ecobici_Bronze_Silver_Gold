# Decisiones del proyecto

Este documento registra cómo se leyeron los archivos originales, qué mapeos se usaron y qué decisiones se tomaron que no están indicadas en el enunciado.

## 1. Diferencias entre años (Tarea 1)

Relevado con `python -m src.exploracion` sobre los archivos originales.

| Aspecto a revisar | 2016 | 2019 | 2024 |
|---|---|---|---|
| Nombre del archivo dentro del ZIP | `recorridos-realizados-2016.csv` | `trips_2019.csv` | `badata_ecobici_recorridos_realizados_2024.csv` |
| Separador de columnas (`,` o `;`) | `,` | `,` (todos los valores entre comillas) | `,` |
| Nombres de las columnas | `periodo`, `genero_usuario`, `fecha_origen_recorrido`, `id_estacion_origen`, `nombre_estacion_origen`, … (sin id de viaje ni de usuario) | Primera columna sin nombre, `Id_recorrido`, `duracion_recorrido`, `fecha_origen_recorrido`, … `id_usuario`, `modelo_bicicleta`, `género` | `id_recorrido`, `duracion_recorrido`, `fecha_origen_recorrido`, … `id_usuario`, `modelo_bicicleta`, `genero` |
| Formato de fecha y hora | `AAAA-MM-DD HH:MM:SS` | `AAAA-MM-DD HH:MM:SS` | `AAAA-MM-DD HH:MM:SS` |
| ¿Trae la duración del viaje? ¿En qué unidad? | Sí, como texto de duración (`0 days 00:16:29.000000000`). **No coincide con las fechas** (coincide en el 3,7 % de los viajes con fecha de fin), por eso se calcula desde las fechas | Sí, en segundos, con separador de miles (`2,814`). Coincide con `fin − inicio` en el 100 % de los casos | Sí, en segundos (`568`). Coincide con `fin − inicio` en el 99,9 % de los casos |
| ¿Trae id de usuario? | No | Sí (`6243BAEcobici`) | Sí (`992557.0`) |
| Cantidad total de filas | 596.807 | 6.217.004 | 3.559.284 |

Usuarios:

| Aspecto a revisar | 2016 | 2019 | 2024 |
|---|---|---|---|
| Nombre del archivo | `usuarios-ecobici-2016.csv` | `usuarios_ecobici_2019.csv` | `usuarios_ecobici_2024.csv` |
| Separador | `,` | `,` | `,` (con BOM UTF-8 al inicio) |
| Nombres de las columnas | `id_usuario`, `genero_usuario`, `edad_usuario`, `fecha_alta`, `hora_alta` | `ID_usuario` (en mayúscula), `genero_usuario`, `edad_usuario`, `fecha_alta`, `hora_alta` | Igual que 2016 |
| Formato de fecha y hora | Fecha `DD-MM-AA` y hora de 12 horas con `AM`/`PM` (`01-01-16`, `3:45:19 AM`) | `AAAA-MM-DD` y `HH:MM:SS` | Igual que 2019 |
| Género | `M`, `F` | `MALE`, `FEMALE`, `OTHER`, vacío | `MALE`, `FEMALE`, `OTHER` |
| Cantidad total de filas | 39.481 | 83.600 | 197.077 |

## 2. Mapeos utilizados

### Viajes (`MAPEO_VIAJES` en `src/silver.py`)

| Columna canónica | 2016 | 2019 | 2024 |
|---|---|---|---|
| `id_viaje` | *(no existe: se genera)* | `Id_recorrido` | `id_recorrido` |
| `fecha_hora_inicio` | `fecha_origen_recorrido` | `fecha_origen_recorrido` | `fecha_origen_recorrido` |
| `fecha_hora_fin` | `fecha_destino_recorrido` | `fecha_destino_recorrido` | `fecha_destino_recorrido` |
| `duracion_seg` | *(se calcula)* | `duracion_recorrido` | `duracion_recorrido` |
| `id_estacion_origen` | `id_estacion_origen` | `id_estacion_origen` | `id_estacion_origen` |
| `nombre_estacion_origen` | `nombre_estacion_origen` | `nombre_estacion_origen` | `nombre_estacion_origen` |
| `id_estacion_destino` | `id_estacion_destino` | `id_estacion_destino` | `id_estacion_destino` |
| `nombre_estacion_destino` | `nombre_estacion_destino` | `nombre_estacion_destino` | `nombre_estacion_destino` |
| `id_usuario` | *(no existe: queda nulo)* | `id_usuario` | `id_usuario` |

### Usuarios (`MAPEO_USUARIOS` en `src/silver.py`)

| Columna canónica | 2016 | 2019 | 2024 |
|---|---|---|---|
| `id_usuario` | `id_usuario` | `ID_usuario` | `id_usuario` |
| `genero` | `genero_usuario` | `genero_usuario` | `genero_usuario` |
| `edad` | `edad_usuario` | `edad_usuario` | `edad_usuario` |
| `fecha_alta` | `fecha_alta` + `hora_alta` (se unen) | `fecha_alta` + `hora_alta` (se unen) | `fecha_alta` + `hora_alta` (se unen) |

### Género (`GENEROS` en `src/silver.py`)

| Valor original | Valor en silver |
|---|---|
| `F`, `FEMALE`, `FEMENINO` | `F` |
| `M`, `MALE`, `MASCULINO` | `M` |
| `OTHER`, `OTRO`, `X` | `OTRO` |
| Vacío o cualquier otro valor | `DESCONOCIDO` |

## 3. Decisiones tomadas

Cada decisión lleva una línea con su justificación.

### Bronze

- **Lectura desde el ZIP con `zipfile`, tomando el primer CSV interno.** Cada ZIP trae un único CSV; el código igual busca el primero con extensión `.csv` por si cambia.
- **Se lee todo como texto y solo el vacío pasa a nulo.** Así no se pierde ni se transforma ningún valor del origen, como pide la consigna.
- **Se lee con `utf-8-sig`, que quita el BOM de los archivos de 2024.** Sin eso, la primera columna se llamaría `﻿id_recorrido` y no coincidiría con el nombre del origen.
- **La primera columna de 2019 queda como `Unnamed: 0`.** En el origen no tiene nombre (es el índice del archivo); pandas le pone ese nombre automáticamente.
- **Escritura por lotes de 500 mil filas con `pyarrow.ParquetWriter`.** El ZIP de 2019 pesa 1,9 GB descomprimido; leerlo completo consumiría mucha memoria.
- **Escritura en archivo temporal y reemplazo al final.** Si el proceso se interrumpe, no queda un Parquet a medias.
- **`_fecha_ingesta` cambia en cada ejecución.** Es un metadato de ingesta; la idempotencia se verifica sobre los datos y los conteos, no sobre esa columna.

### Silver

- **`id_viaje` lleva el año como prefijo (`2019-9379`).** Los ids de 2019 y 2024 usan numeraciones distintas; con el prefijo no pueden colisionar y la regla DUPLICADO funciona entre años.
- **Se quita el sufijo `BAEcobici` y el `.0` de los ids.** Son el mismo id con formato distinto entre años; sin quitarlos no se pueden comparar ni convertir a entero.
- **En 2016 el id de viaje se genera por orden de llegada en bronze** (`2016-0000001`, …). El archivo no trae id de viaje, y el orden del archivo es fijo, así que el resultado es reproducible.
- **En 2016 la duración se calcula como `fin − inicio`.** La duración declarada no coincide con las fechas (correlación prácticamente nula), y la consigna pide calcularla en esos casos.
- **En 2019 y 2024 se usa la duración declarada.** Coincide con `fin − inicio` en el 100 % (2019) y el 99,9 % (2024) de los viajes.
- **Los nombres de estación pierden tildes, código inicial y espacios repetidos.** Un mismo id aparece como `005 - Plaza Italia` en 2019 y como `Plaza Italia` en 2016; sin esta normalización no coinciden. Las tildes también varían entre años (`PACÍFICO` / `PACIFICO`).
- **Las edades fuera de 16 a 90 quedan nulas en lugar de rechazar el registro.** Lo pide la consigna; el registro sigue siendo un usuario válido.
- **Se agregan dos reglas de calidad para usuarios: `ID_NULO` y `FECHA_INVALIDA`.** Sin id no hay clave para usar el registro, y sin fecha de alta no se puede decidir cuál duplicado conservar.
- **Los usuarios de los tres años se unifican antes de buscar duplicados.** Un mismo id puede aparecer en archivos de años distintos; ante un duplicado se conserva el alta más reciente, como pide la consigna.
- **Los viajes se controlan por año, no en conjunto.** Como los ids llevan el año como prefijo, no hay duplicados entre años y se puede procesar cada año por separado, con menos memoria.
- **Las tablas silver se guardan particionadas por año (`anio=AAAA/data.parquet`).** Igual que bronze: permite reprocesar un año sin tocar los demás.

### Gold

- **`dim_fecha` cubre todos los días entre el primer y el último viaje, sin huecos.** Así cada fecha de `fact_viajes` existe en la dimensión, aunque no haya viajes ese día.
- **El día de la semana se escribe con una lista fija en español.** Depender del idioma configurado en el sistema haría que el resultado cambie de una máquina a otra.
- **`dim_estacion` toma el nombre del año más reciente en que aparece la estación.** Es lo que pide la consigna; si ese año tiene más de un nombre, gana el más frecuente.
- **Los rangos etarios son `16-25`, `26-35`, `36-50` y `51+`, con los bordes incluidos en el rango menor.** Así `25` queda en `16-25` y `35` en `26-35`, sin solapamientos.
- **`duracion_min` se redondea a un decimal.** Lo pide la consigna; la duración en segundos sigue disponible en silver.
- **`dim_usuario` sale solo de silver/usuarios.** Gold no reconstruye usuarios desde los viajes, porque los viajes no traen edad ni fecha de alta.

### Reportes

- **Los viajes sin usuario conocido se agrupan como `DESCONOCIDO` en el reporte 3.** Son la mayoría de los viajes; ver la sección 4.
- **Los empates del top 10 se ordenan por id de estación.** Así dos ejecuciones dan exactamente el mismo CSV.
- **El reporte de calidad lee bronze y silver directamente.** Es la excepción que indica la consigna: necesita contar filas de cada capa.

## 4. Resultados y observaciones de calidad

### Cuarentena de viajes

| Año | Válidos | Cuarentena | Motivo principal |
|---|---|---|---|
| 2016 | 503.252 | 93.555 (15,68 %) | `FECHA_INVALIDA`: la fecha de fin viene vacía |
| 2019 | 6.207.851 | 9.153 (0,15 %) | `DURACION_MAYOR_24H` (7.393) y `ESTACION_NULA` (1.760) |
| 2024 | 3.555.217 | 4.067 (0,11 %) | `FECHA_INVALIDA` (3.379), `DURACION_MAYOR_24H` (687) y `DUPLICADO` (1) |

Ningún viaje se pierde: en cada año, las filas de bronze son iguales a las de silver más las de cuarentena.

### Cuarentena de usuarios

Los 5.231 usuarios rechazados son todos del archivo de 2016 y figuran como `DUPLICADO`: el mismo id aparece en 2019 con otra fecha de alta, y se conserva la más reciente. Puede tratarse de una reutilización de ids en la fuente; no se intentó corregirla.

### Viajes sin usuario en `dim_usuario`

Son 6.623.887 de los 10.266.320 viajes válidos:

- **503.252 viajes de 2016 no tienen id de usuario**, porque el archivo no lo trae.
- **6.120.635 viajes de 2019 y 2024 tienen un id de usuario que no está en `dim_usuario`**: 4.410.500 de 2019 (71 %) y 1.710.135 de 2024 (48 %).

Explicación posible: cada archivo de usuarios trae solo las altas de su año. Muchos de los viajes corresponden a personas dadas de alta en años que no están en la fuente, así que su id no tiene correspondencia en `dim_usuario`. Esto no es un error del modelo: el id existe en los viajes y se conserva como clave.

### Respuesta a la pregunta del reporte 2

¿Cambian las estaciones con más viajes de un año a otro? **Sí, pero no del todo.** En el top 10 coinciden 5 estaciones entre 2016 y 2019, 5 entre 2019 y 2024, y 3 entre 2016 y 2024. `CONSTITUCION` pasa a ser la primera en 2019 y 2024, y `PACIFICO` está entre las dos primeras en esos años. `ERRONEA` aparece en el top de 2016 (ver siguiente punto).

### Anomalías de la fuente que no se corrigieron

- **La estación `ERRONEA` (id 1) tiene 12.600 viajes en 2016**, y figura séptima en el top de ese año. Es un nombre que viene así del origen; no hay forma de saber cuál es la estación real, así que se mantiene.
- **En 2016 aparece el nombre `KP1808000368`**, que es un código y no un nombre. Se mantiene como viene.
- **La duración declarada de 2016 no coincide con las fechas.** Se calcula desde las fechas (ver sección 3), pero la fuente sigue siendo inconsistente.

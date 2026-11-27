# OceanWatch Analytics — AIS MarineCadastre

Proyecto final de **Soluciones Intensivas en Datos (MINE 4213)**. El repo contiene las dos
entregas:

- **Entrega 1** (carpeta [`notebooks/`](notebooks/)): explora el tráfico marítimo AIS de una
  semana (2023-06-01 a 2023-06-07), responde 5 preguntas de negocio del enunciado, agrega una
  pregunta adicional sobre visitas a puerto, y compara formatos y layouts de almacenamiento sobre
  Delta Lake / Unity Catalog en Databricks.
- **Entrega 2** (carpeta [`pipeline/`](pipeline/)): el pipeline completo — ingesta en streaming,
  capas con calidad como compuerta, orquestación y ML. Ver
  [Entrega 2 — pipeline en streaming](#entrega-2--pipeline-en-streaming).

## Entrega 2 — pipeline en streaming

### Arquitectura

```
 NOAA (zip diario) ──► 01_productor ──(subida atómica, 1 CSV/día)──► landing/ais/entrantes
                                                                            │ Auto Loader
 WPI · Natural Earth · semilla ──► 00_preparacion ──► silver.dim_*          ▼
                                          │                  02_bronze ──► bronze (crudo, append)
                                          │                                 │ foreachBatch + MERGE
                                          └──────── compuertas ───► 03_silver ──► silver ─┬─► cuarentena
                                                                            │              └─► métricas de calidad
                                                                            ▼
                                                    04_gold ──► gold (recalculado) + chequeo de salud
                                                                            │
                                                                   05_ml ◄──┘
```

### Índice de notebooks

Cada notebook es una tarea del job y sigue la misma estructura: *Propósito → Entradas y salidas
→ Decisiones → Código → Evidencia*.

| # | Notebook | Capa / rol | Lee | Escribe | Estado |
|---|---|---|---|---|---|
| 0 | [`00_preparacion`](pipeline/00_preparacion.py) | Estructura + referencias | WPI, Natural Earth, semilla | catálogo, esquemas, Volumes, `silver.dim_*` | corrido OK (2026-11-25) |
| 1 | [`01_productor`](pipeline/01_productor.py) | Simulador del feed (param. `fecha`) | NOAA | `landing/ais/entrantes` | corrido OK (2026-11-25) |
| 2 | [`02_bronze`](pipeline/02_bronze.py) | Bronce | `landing/ais/entrantes` | `bronze.ais_posiciones_raw` | corrido OK (2026-11-25) |
| 3 | [`03_silver`](pipeline/03_silver.py) | Plata (compuertas) | `bronze`, `silver.dim_*` | `silver` + cuarentena + métricas | ejecutado 2023-06-01 (corre en Databricks: `foreachBatch`) |
| 4 | [`04_gold`](pipeline/04_gold.py) | Oro (batch incremental) + chequeo de salud | `silver` | `gold` | ejecutado 2023-06-01 (corre también en local) |
| 5 | `05_ml` | ML con MLflow | `gold` | experimento + modelo en UC | pendiente |

Configuración compartida (nombres, rutas, fuentes, parámetros): [`pipeline/config.py`](pipeline/config.py).

### Principios de diseño

1. **Modelo unificado.** El resultado de plata y oro es función del *conjunto* de eventos
   recibidos, no de cómo llegaron (un archivo diario o por hora, repetidos, tardíos, o
   reprocesados tras perder el checkpoint). El mismo código sirve para streaming y backfill.
2. **Unidad de idempotencia por capa.** Bronce: el archivo (checkpoint de Auto Loader +
   append). Plata: el registro `(MMSI, H3_Macro, BaseDateTime)`, vía `MERGE` con un ganador
   determinístico (calidad → completitud → hash como último desempate). Oro: la llave del
   agregado, recalculada desde plata.
3. **Plata = transformaciones por fila; oro = transformaciones entre filas** (trayectorias,
   agregados), recalculadas para las llaves afectadas.
4. **Sobrescribir solo es seguro con estados completos** (dimensiones de referencia, días de
   oro recalculados desde plata); con incrementos, `MERGE` por llave.
5. **Bronce absorbe cambios de esquema sin perder datos** (strings + `_rescued_data`); plata
   tiene un contrato fijo; un cambio de esquema queda medido (`filas_esquema_rescatado` en las
   métricas de calidad) y se reporta, no rompe el flujo.
6. **Los datos tardíos se corrigen recalculando, no con watermark.** Un evento tardío entra a
   plata por `MERGE` y oro recalcula su fecha. Así el resultado es exacto aunque los datos
   lleguen tarde; el enunciado no pide alertas en tiempo real (las "anomalías del día" salen de
   oro).
7. **PySpark como lenguaje base**, igual que en la Entrega 1: toda transformación usa la API de
   DataFrame (`pyspark.sql.functions as f`; las funciones H3 y espaciales nativas de Databricks
   vía `pyspark.databricks.sql.functions as dbf`). `spark.sql` solo para DDL y metadatos de
   Unity Catalog (`CREATE`, `COMMENT ON`, `SHOW`), que no tienen equivalente en la API.

### Unity Catalog (Entrega 2)

```
oceanwatch_lakehouse                     (catálogo; parámetro `catalogo`)
├── landing   Volumes: ais/{_descargas,entrantes} · referencias/{wpi,natural_earth,semillas}
├── bronze    crudo inmutable
├── silver    datos validados · cuarentena · métricas · dim_tipo_buque · dim_puertos · dim_h3_tierra_firme
├── gold      modelo de negocio
└── ops       Volume checkpoints (estado de los streams y esquemas de Auto Loader)
```

### Cómo ejecutar la Entrega 2

- **En Databricks:** Git folder del repo, cómputo serverless, correr los notebooks de
  `pipeline/` en orden (o el job, cuando esté definido).
- **En local:** `uv sync`, kernel del `.venv` en VS Code y la extensión de Databricks conectada
  con el perfil `uniandes`. `config.obtener_spark()` usa Databricks Connect (el cómputo corre en
  el workspace) y `config.obtener_workspace()` la Files API para leer y escribir en Volumes.

## Cómo leer este repo (Entrega 1)

Cada notebook tiene sus propias celdas `%md` con el detalle de cada decisión (por qué un filtro,
qué muestra un `.explain()`, etc.). Este README no repite ese contenido: da el mapa general —
cómo se ejecutan los notebooks en orden, qué produce cada uno, y dónde vive cada tabla en Unity
Catalog — para que sea fácil ubicarse antes de entrar al detalle de cada celda.

## Cómo ejecutar

1. Abre el repo como una **carpeta Git en Databricks** (Workspace → Create → Git folder), con
   compute **Serverless**.
2. Ejecuta los notebooks **en orden** (`01` → `02` → `03`/`04`, ver dependencias abajo). Cada uno
   asume que el anterior ya corrió al menos una vez.
3. En local (fuera de Databricks), los notebooks crean su propia `SparkSession` vía
   `databricks-connect` con el perfil `uniandes` (`.databrickscfg`). Ese modo sirve para editar y
   revisar el código, pero **la descarga (`01`) solo debe correr dentro de Databricks**, porque
   escribe directamente en `/Volumes/...`, una ruta que en local no existe.

## Distribución de los notebooks

| Notebook | Qué hace | Requisito(s) del enunciado |
|---|---|---|
| [`01_ingesta.py`](notebooks/01_ingesta.py) | Crea el catálogo/esquemas/Volume en Unity Catalog. Descarga los 7 zip de AIS (con reintentos y verificación de integridad) y el World Port Index, y descomprime los CSV en el Volume. | Requisito 1 (ingesta) |
| [`02_eda_silver.py`](notebooks/02_eda_silver.py) | Lee los CSV con esquema explícito, explora y cuantifica problemas de calidad sobre los datos crudos, y construye la capa `silver`: limpieza no destructiva, enriquecimiento H3, segmentación de viajes, y las tablas de referencia (`world_port_index`, `vessel_types`). | Requisito 2 (exploración y calidad) |
| [`03_preguntas_negocio.py`](notebooks/03_preguntas_negocio.py) | Responde las 5 preguntas de negocio (a-e) leyendo directamente las tablas `silver`. No reconstruye nada, solo consulta. | Requisito 3 (preguntas de negocio) |
| [`04_almacenamiento.py`](notebooks/04_almacenamiento.py) | Pregunta de negocio adicional (visitas a puerto: tráfico, ocupación, tiempo en tierra) construida como *accumulating snapshot fact*, y evidencia de almacenamiento óptimo (Parquet/Delta, `CLUSTER BY`, archivos leídos antes/después). | Pregunta adicional + Requisito 4 (almacenamiento) |

**Dependencias entre notebooks:** `02` necesita que `01` haya corrido al menos una vez (para que
existan los CSV y el WPI en el Volume). `03` y `04` necesitan que `02` haya corrido al menos una
vez (leen `silver.ais_cleaned`, `silver.world_port_index` y `silver.vessel_types` por nombre, no
recalculan nada). `03` y `04` son independientes entre sí.

## Datos y fuentes

- **AIS (posiciones):** [MarineCadastre / NOAA](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/),
  7 archivos diarios (2023-06-01 a 2023-06-07), descargados por `01_ingesta.py`.
- **World Port Index:** [NGA Pub. 150](https://msi.nga.mil/Publications/WPI), CSV descargado
  automáticamente por `01_ingesta.py`.
- **Catálogo de tipos de buque:** estándar AIS/NAIS, según
  [NOAA VesselTypeCodes2018.pdf](https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf).
  Recreado a mano como tabla (`silver.vessel_types`); el PDF no se descarga porque ningún paso del
  pipeline lo consume.

## Unity Catalog: estructura

```
oceanwatch                             (catálogo)
├── raw
│   └── ais_files                      (Volume: positions/zips, positions/csvs, reference/)
├── silver
│   ├── ais_cleaned                    (posiciones limpias + enriquecimiento H3 + viajes)
│   ├── world_port_index               (catálogo de puertos, ajustado)
│   └── vessel_types                   (catálogo de tipos de buque)
└── gold
    ├── visitas_puerto                 (accumulating snapshot fact, CLUSTER BY wpi_id, Fecha_Entrada)
    └── visitas_puerto_sin_cluster     (misma tabla sin CLUSTER BY, solo para comparar Requisito 4)
```

Catálogo, esquemas, Volume y las 5 tablas tienen `COMMENT` en Unity Catalog — se pueden consultar
con `DESCRIBE CATALOG/SCHEMA/TABLE EXTENDED` o desde el Catalog Explorer del workspace, sin tener
que abrir el código.

## Bitácora

El aporte de cada sesión de trabajo queda en el historial de commits del repositorio
(`git log`), con mensajes descriptivos por avance. [`BITACORA.md`](BITACORA.md) complementa esa
bitácora justificando, técnica por técnica, qué se usó de las sesiones presenciales (semana 5:
Spark a escala; semana 6: formatos de almacenamiento) y dónde quedó aplicado en el código —
incluyendo lo que se vio en clase pero **no** se necesitó en esta entrega (time travel, `MERGE`,
`OPTIMIZE`, `VACUUM`) y por qué.

## Requisitos técnicos

Ver [`pyproject.toml`](pyproject.toml) (dependencias, `databricks-connect`) y
[`databricks.yml`](databricks.yml) (configuración del bundle). El entorno local se maneja con
`uv` (`uv sync`).

## Uso de herramientas de IA

> Se utilizó Claude (Anthropic, 2026) como herramienta de apoyo para estructurar el análisis de
> los casos, generar ideas y apoyar la elaboración de propuestas. El contenido fue revisado,
> validado y adaptado por los autores del trabajo.

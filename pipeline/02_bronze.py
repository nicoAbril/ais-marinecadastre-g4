# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 · Bronce: ingesta incremental del feed AIS con Auto Loader
# MAGIC
# MAGIC **OceanWatch Analytics · Entrega 2** · Tarea 3 del job · Mapa completo del pipeline en el
# MAGIC [README](../README.md#entrega-2--pipeline-en-streaming).
# MAGIC
# MAGIC ## Propósito
# MAGIC
# MAGIC Llevar cada archivo que llega a `landing/ais/entrantes/` a la tabla de bronce **tal como
# MAGIC vino**, exactamente una vez, con trazabilidad del archivo de origen. Bronce es el registro de
# MAGIC llegadas: no limpia, no deduplica, no tipa. Eso es trabajo de plata.
# MAGIC
# MAGIC ## Entradas y salidas
# MAGIC
# MAGIC | | Objeto | Detalle |
# MAGIC |---|---|---|
# MAGIC | **Entrada** | `landing/ais/entrantes/*.csv` | Un CSV completo por día (más reenvíos e inyectados de demostración) |
# MAGIC | **Salida** | `bronze.ais_posiciones_raw` | Todas las columnas de la fuente como `STRING` + `_rescued_data` + columnas técnicas |
# MAGIC | **Estado** | `ops/checkpoints/bronze` | Qué archivos ya se leyeron (checkpoint de Structured Streaming) |
# MAGIC | **Estado** | `ops/checkpoints/autoloader_schemas/bronze` | Esquema detectado por Auto Loader y su historial |
# MAGIC
# MAGIC **Parámetros:** `catalogo`.
# MAGIC
# MAGIC ## Decisiones
# MAGIC
# MAGIC - **Auto Loader + `trigger(availableNow=True)`.** Procesa todo lo pendiente y termina: el
# MAGIC   streaming funciona como ETL incremental que el job dispara una vez al día. Es además el
# MAGIC   único trigger de serverless.
# MAGIC - **Unidad de idempotencia: el archivo.** El checkpoint guarda qué archivos ya se
# MAGIC   procesaron; volver a correr no los relee. Escritura en `append`.
# MAGIC - **Sin esquema declarado; todo `STRING`.** Auto Loader toma los nombres del encabezado del
# MAGIC   CSV y, para CSV, deja todas las columnas como texto (no infiere tipos). Así bronce es un
# MAGIC   espejo fiel de la fuente y ningún valor se pierde por una conversión fallida. El contrato
# MAGIC   (qué columnas y de qué tipo) vive en plata, que selecciona solo las que conoce.
# MAGIC - **Columnas por nombre, no por posición (verificado).** Se probó un CSV con `LON` antes
# MAGIC   que `LAT`: Auto Loader asignó cada valor a su columna por el encabezado, con esquema
# MAGIC   explícito y con esquema inferido. Un reordenamiento en la fuente no desalinea los datos.
# MAGIC - **Cambios de esquema: modo `rescue`.** Una columna nueva en la fuente no detiene el stream
# MAGIC   ni se pierde: su valor queda en `_rescued_data` (JSON con nombre y valor). Una columna que
# MAGIC   desaparece queda nula. Plata cuenta esas filas por fecha (`filas_esquema_rescatado` en
# MAGIC   `calidad_metricas`) y el chequeo de salud de oro lo reporta, sin parar la ingesta. Se descartó `addNewColumns` porque detiene el stream a propósito ante cada
# MAGIC   columna nueva y depende de un reintento: contradice "un cambio de esquema genera alerta,
# MAGIC   no rompe el flujo".
# MAGIC - **Un archivo por micro-lote** (`maxFilesPerTrigger = 1`): cada lote del historial
# MAGIC   corresponde a un archivo, lo que hace legible la evidencia de tolerancia a fallos.
# MAGIC - **Inmutabilidad impuesta por el motor:** `delta.appendOnly = true`. Delta rechaza
# MAGIC   cualquier `UPDATE` o `DELETE` sobre bronce; "crudo inmutable" deja de ser convención.

# COMMAND ----------

from pyspark.sql import functions as f

import config

spark = config.obtener_spark()

CATALOGO = config.leer_parametro("catalogo", config.CATALOGO_DEFECTO)
cfg = config.Config(CATALOGO)

T_BRONZE = cfg.tabla("bronze", "ais_posiciones_raw")
RUTA_ENTRANTES = cfg.ruta_ais_entrantes
CHECKPOINT = cfg.ruta_checkpoint("bronze")
ESQUEMA_AUTOLOADER = cfg.ruta_esquema_autoloader("bronze")
print(f"{RUTA_ENTRANTES}  ->  {T_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Definición del stream
# MAGIC
# MAGIC Las cuatro piezas fijas de Structured Streaming: fuente (`readStream` de Auto Loader),
# MAGIC transformación (solo columnas técnicas), destino (`toTable`) y memoria (`checkpointLocation`).
# MAGIC
# MAGIC Columnas técnicas (prefijo `_` para separarlas de las de la fuente):
# MAGIC
# MAGIC | Columna | Origen | Para qué |
# MAGIC |---|---|---|
# MAGIC | `_archivo` | `_metadata.file_name` | Linaje: de qué archivo vino cada fila (reenvíos e inyectados se reconocen aquí) |
# MAGIC | `_archivo_modificado` | `_metadata.file_modification_time` | Cuándo llegó el archivo a landing |
# MAGIC | `_ingesta_ts` | `current_timestamp()` | Cuándo entró la fila a bronce |
# MAGIC | `_rescued_data` | Auto Loader (modo `rescue`) | Valores que no encajan en el esquema (columnas nuevas) |

# COMMAND ----------

def stream_bronce():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", ESQUEMA_AUTOLOADER)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .option("pathGlobFilter", "*.csv")
        .load(RUTA_ENTRANTES)
        .withColumn("_archivo", f.col("_metadata.file_name"))
        .withColumn("_archivo_modificado", f.col("_metadata.file_modification_time"))
        .withColumn("_ingesta_ts", f.current_timestamp())
    )


def correr_bronce(nombre_consulta: str) -> list[dict]:
    """Procesa todo lo pendiente (availableNow) y devuelve el progreso de cada micro-lote."""
    consulta = (
        stream_bronce()
        .writeStream.queryName(nombre_consulta)
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
        .toTable(T_BRONZE)
    )
    consulta.awaitTermination()
    return list(consulta.recentProgress)



def version_actual(tabla: str) -> int:
    """Última versión de la tabla Delta (-1 si todavía no existe)."""
    if not spark.catalog.tableExists(tabla):
        return -1
    return spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 1").first().version

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingesta

# COMMAND ----------

v_antes = version_actual(T_BRONZE)
progreso = correr_bronce("bronze_ingesta")

# Evidencia desde el historial de Delta: una versión STREAMING UPDATE por micro-lote (un archivo
# por lote). No depende de recentProgress, que en serverless puede venir vacío.
lotes = (
    spark.sql(f"DESCRIBE HISTORY {T_BRONZE}")
    .filter((f.col("version") > v_antes) & (f.col("operation") == "STREAMING UPDATE"))
    .select(
        "version",
        f.col("operationParameters.epochId").alias("lote"),
        f.col("operationMetrics.numOutputRows").cast("long").alias("filas"),
        "timestamp",
    )
    .orderBy("version")
)
lotes.show(truncate=False)
filas_ingesta = lotes.agg(f.sum("filas")).first()[0] or 0
print(f"Micro-lotes en esta corrida: {lotes.count()} · filas ingeridas: {filas_ingesta:,}")

# Diagnóstico: ¿recentProgress trae datos en este entorno? (solo informativo)
print(f"recentProgress reporta {sum((p.numInputRows or 0) for p in progreso):,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Contrato y gobernanza de la tabla
# MAGIC
# MAGIC La tabla la crea el primer `toTable` con el esquema del encabezado; aquí se le fija la
# MAGIC documentación y la propiedad de solo-append. Todo es idempotente (se puede repetir cada
# MAGIC corrida).

# COMMAND ----------

spark.sql(f"ALTER TABLE {T_BRONZE} SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
spark.sql(f"""
    COMMENT ON TABLE {T_BRONZE} IS
    'Capa bronce del feed AIS (NOAA MarineCadastre): cada CSV de landing/ais/entrantes tal como
    llegó, exactamente una vez (Auto Loader + checkpoint). Todas las columnas de la fuente como
    STRING; valores fuera del esquema en _rescued_data. Solo append (delta.appendOnly).'
""")
for columna, comentario in {
    "_archivo": "Nombre del archivo de origen en landing/ais/entrantes",
    "_archivo_modificado": "Fecha de modificación del archivo en landing (llegada)",
    "_ingesta_ts": "Momento en que la fila entró a bronce",
    "_rescued_data": "JSON con valores que no encajaron en el esquema (p. ej. columnas nuevas de la fuente)",
}.items():
    spark.sql(f"ALTER TABLE {T_BRONZE} ALTER COLUMN {columna} COMMENT '{comentario}'")
print("Propiedades y comentarios aplicados.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Evidencia
# MAGIC
# MAGIC ## Idempotencia del checkpoint
# MAGIC
# MAGIC # Evidencia
# MAGIC
# MAGIC ## Idempotencia del checkpoint
# MAGIC
# MAGIC Se lanza el mismo stream otra vez, de inmediato. Como el checkpoint ya registró todos los
# MAGIC archivos, **no debe escribir nada**: la versión de la tabla Delta tiene que ser la misma antes
# MAGIC y después. Se compara la versión y no `recentProgress`, porque en serverless `recentProgress`
# MAGIC puede llegar vacío y entonces "0 filas" se cumpliría siempre, aunque el checkpoint fallara.
# MAGIC Esta verificación corre en cada ejecución del job: si alguna vez se escribiera algo, la tarea falla.

# COMMAND ----------

v_antes = version_actual(T_BRONZE)
correr_bronce("bronze_verificacion")
v_despues = version_actual(T_BRONZE)
print(f"Versión de bronce antes: {v_antes} · después de la segunda corrida: {v_despues}")
assert v_despues == v_antes, "La segunda corrida escribió en bronce: el checkpoint no evitó releer archivos"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contenido de bronce por archivo
# MAGIC
# MAGIC Una fila por archivo procesado. `filas_rescued` > 0 indicaría que la fuente envió columnas
# MAGIC fuera del esquema (plata la registra como `filas_esquema_rescatado`).

# COMMAND ----------

(
    spark.table(T_BRONZE)
    .groupBy("_archivo")
    .agg(
        f.count("*").alias("filas"),
        f.count("_rescued_data").alias("filas_rescued"),
        f.min("_ingesta_ts").alias("ingerido"),
    )
    .orderBy("_archivo")
    .show(truncate=False)
)

print("Esquema detectado por Auto Loader:")
spark.table(T_BRONZE).printSchema()
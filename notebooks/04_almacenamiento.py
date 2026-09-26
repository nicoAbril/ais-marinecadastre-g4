# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 - Visitas a puerto y almacenamiento óptimo (Requisito 4)
# MAGIC
# MAGIC Este notebook combina una pregunta de negocio adicional (más allá de las 5 obligatorias del
# MAGIC punto 3) con el Requisito 4 del enunciado (almacenamiento óptimo para un propósito): la tabla
# MAGIC que responde la pregunta de negocio es, a la vez, el propósito de consulta concreto sobre el
# MAGIC que se demuestra la mejora de formato y layout.

# COMMAND ----------

try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile("uniandes").serverless(True).getOrCreate()

from pyspark.sql import functions as f
from pyspark.sql import Window

CATALOG = 'oceanwatch'

# Capa fuente: donde ya viven las posiciones limpias y el catálogo de puertos
SOURCE_SCHEMA = 'silver'
SOURCE_TABLE_AIS = 'ais_cleaned'
SOURCE_TABLE_WPI = 'world_port_index'

# Capa destino: donde queda la tabla de visitas a puerto (accumulating snapshot / OBT)
DEST_SCHEMA = 'gold'
DEST_TABLE_VISITAS = 'visitas_puerto'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta de negocio: visitas a puerto y ocupación
# MAGIC
# MAGIC **Pregunta:** ¿qué puertos concentran más tráfico de entrada/salida, en qué franjas horarias
# MAGIC se concentra la ocupación, y cuánto tiempo permanece un buque en puerto en promedio?
# MAGIC
# MAGIC **Por qué no está en el punto 3:** las 5 preguntas obligatorias trabajan sobre eventos ya
# MAGIC existentes en `silver.ais_cleaned` (posiciones, distancias, tipos de buque). Esta pregunta
# MAGIC necesita un dato que no existe todavía en los datos crudos: **cuándo entró y cuándo salió** un
# MAGIC buque de un puerto. Eso hay que derivarlo detectando transiciones de estado sobre la secuencia
# MAGIC de posiciones de cada buque, con la misma técnica de ventana (`lag()` + bandera de cambio) que
# MAGIC ya se usó para segmentar viajes (`Flag_Nuevo_Viaje` / `Trip_ID`) en la capa Silver.
# MAGIC
# MAGIC **Detección de "está en puerto":** se reutiliza el cruce por celda H3 (`h3_kring` alrededor del
# MAGIC punto del World Port Index) ya construido para la pregunta 3d, en vez de un cálculo de distancia
# MAGIC haversine contra cada puerto. Es una aproximación (hexagonal, no un radio exacto), suficiente
# MAGIC para saber si una posición cae dentro del área de un puerto, sin necesitar precisión de metros.
# MAGIC
# MAGIC **Modelo:** una *accumulating snapshot fact* como tabla ancha (OBT, sin dimensiones separadas
# MAGIC ni validación de integridad referencial — no es el objetivo aquí). Grano: una fila por visita
# MAGIC (buque, puerto, entrada, salida), con `fecha_entrada`, `hora_entrada`, `fecha_salida`,
# MAGIC `hora_salida` y `duracion_horas`, más atributos descriptivos denormalizados del buque y del
# MAGIC puerto.
# MAGIC
# MAGIC **Propósito de consulta declarado (Requisito 4):** filtrar y agrupar por puerto y por fecha de
# MAGIC entrada — el caso de "cuánto tráfico y qué ocupación tuvo este puerto en este rango de fechas".
# MAGIC Layout esperado a comparar: `CLUSTER BY (wpi_id, fecha_entrada)` frente a la tabla sin ese
# MAGIC layout, con evidencia de archivos leídos y bytes antes/después.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fase 1: puertos expandidos y desambiguados (`df_wpi_unico`)
# MAGIC
# MAGIC Cada puerto del WPI es un punto; se expande a un anillo de 5 hexágonos H3 (~4 km) para que las
# MAGIC posiciones cercanas hagan match, y se desambigua cuando dos puertos vecinos comparten una
# MAGIC celda, quedándose con el más cercano (misma lógica que la pregunta 3d).

# COMMAND ----------

w_desambiguacion = Window.partitionBy("H3_Res8").orderBy("Hexagonos_Al_Centro_WPI", "wpi_id")

df_wpi_unico = (
    spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE_WPI}")
    .filter(f.col("lat").isNotNull() & f.col("lon").isNotNull())
    .withColumn("H3_Puerto_Centro", f.expr("h3_longlatash3(lon, lat, 8)"))
    .withColumn("H3_Res8", f.expr("explode(h3_kring(H3_Puerto_Centro, 5))"))
    .withColumn("Hexagonos_Al_Centro_WPI", f.expr("h3_distance(H3_Puerto_Centro, H3_Res8)"))
    .withColumn("rn", f.row_number().over(w_desambiguacion))
    .filter(f.col("rn") == 1)
    .drop("rn")
)

display(df_wpi_unico)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fase 2: posiciones con detección "en puerto" (`df_ais_en_puerto`)
# MAGIC
# MAGIC Se proyecta cada posición a H3 resolución 8 y se cruza por igualdad de celda contra
# MAGIC `df_wpi_unico`. `en_puerto` exige dos condiciones, no solo la geográfica: la celda cayó dentro
# MAGIC del buffer de algún puerto (`wpi_id` no nulo), y el propio buque reporta `Status` de fondeado
# MAGIC (1) o amarrado (5) — no solo "cerca" mientras navega. Sin el filtro de `Status`, un buque que
# MAGIC pasa a velocidad de crucero cerca del borde del buffer contaría como una "visita", cuando en
# MAGIC realidad nunca atracó.
# MAGIC
# MAGIC **`Status` nulo se trata como "sin información", no como "no está fondeado":** `col.isin(...)`
# MAGIC devuelve `null` si `col` es `null`, y `null & True = null`. Sin el `isNull()` de respaldo, un
# MAGIC solo ping sin `Status` dentro de una visita real quedaría con `en_puerto = null`. Como el
# MAGIC `lag()` usa `coalesce(prev_en_puerto, False)`, ese `null` se leería como "no estaba en puerto" y
# MAGIC partiría una sola visita en dos (una entrada y una salida fantasma) por un dato faltante, no
# MAGIC por un evento real.

# COMMAND ----------

EN_PUERTO_STATUS = [1, 5]  # 1 = fondeado (at anchor), 5 = amarrado (moored)

df_ais_en_puerto = (
    spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE_AIS}")
    .withColumn("H3_Res8", f.expr("h3_longlatash3(LON, LAT, 8)"))
    .join(df_wpi_unico, on="H3_Res8", how="left")
    .withColumn("en_puerto",
        f.col("wpi_id").isNotNull()
        & (f.col("Status").isNull() | f.col("Status").isin(EN_PUERTO_STATUS))
    )
)

display(df_ais_en_puerto)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plan de ejecución: join AIS x puertos expandidos
# MAGIC
# MAGIC `df_wpi_unico` tiene hasta ~91 celdas por puerto (antes de desambiguar); a diferencia de
# MAGIC `vessel_types` (~100 filas), no es obvio si Spark lo sigue difundiendo (`BroadcastHashJoin`) o
# MAGIC si ya pesa lo suficiente para forzar un shuffle de la tabla grande (`ais_cleaned`).

# COMMAND ----------

df_ais_en_puerto.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fase 3: detección de transiciones entrada/salida (`df_transiciones`)
# MAGIC
# MAGIC Se compara `en_puerto` contra su valor anterior (`lag`) por buque y tiempo: `False -> True` es
# MAGIC una entrada, `True -> False` es una salida. `Visita_ID` es la suma acumulada de entradas: agrupa
# MAGIC todas las filas de una misma estancia bajo el mismo número de visita (misma técnica que
# MAGIC `Trip_ID` en Silver).

# COMMAND ----------

w_viaje = Window.partitionBy("MMSI_Real").orderBy("BaseDateTime")
w_acum = Window.partitionBy("MMSI_Real").orderBy("BaseDateTime").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_transiciones = (
    df_ais_en_puerto
    .withColumn("prev_en_puerto", f.lag("en_puerto").over(w_viaje))
    .withColumn(
        "Flag_Entrada",
        (f.col("en_puerto") == True) & (f.coalesce(f.col("prev_en_puerto"), f.lit(False)) == False)
    )
    .withColumn(
        "Flag_Salida",
        (f.col("en_puerto") == False) & (f.coalesce(f.col("prev_en_puerto"), f.lit(False)) == True)
    )
    .withColumn("Visita_ID", f.sum(f.col("Flag_Entrada").cast("int")).over(w_acum))
)

display(df_transiciones)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plan de ejecución: ventanas de detección (lag + suma acumulada)
# MAGIC
# MAGIC `w_viaje` y `w_acum` comparten el mismo `partitionBy`/`orderBy` (`MMSI_Real`, `BaseDateTime`).
# MAGIC Se confirma si Spark reutiliza el mismo shuffle/sort para ambas ventanas (un solo *Exchange*) o
# MAGIC si calcula dos por separado — la misma pregunta que quedó pendiente al construir `Trip_ID`.

# COMMAND ----------

df_transiciones.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fase 4: consolidación a grano de visita (`df_visitas`)
# MAGIC
# MAGIC Una fila por `(MMSI_Real, Visita_ID)`: `entrada_ts`/`salida_ts` de la estancia, atributos
# MAGIC descriptivos del buque y del puerto, y las banderas de censura para visitas truncadas en los
# MAGIC bordes de la semana. `salida_ts` sale de la fila `Flag_Salida` (la salida real), no de la
# MAGIC última posición dentro del puerto — queda `null` si nunca se observó, en vez de inventar un
# MAGIC valor. `Visita_SK` es una llave determinística (hash de `MMSI_Real` + `entrada_ts`): mismos
# MAGIC datos de entrada dan siempre el mismo hash, a diferencia de un UUID aleatorio en cada corrida.

# COMMAND ----------

df_visitas = (
    df_transiciones
    .groupBy("MMSI_Real", "Visita_ID")
    .agg(
        # Atributos descriptivos del buque: se pierden al agrupar si no se agregan explícitamente
        f.first("MMSI", ignorenulls=True).alias("MMSI"),
        f.first("VesselName", ignorenulls=True).alias("VesselName"),
        f.first("VesselType", ignorenulls=True).alias("VesselType"),
        f.first("MMSI_Anomalo", ignorenulls=True).alias("MMSI_Anomalo"),
        # Tamaño del buque, numérico (sin categorizar): Length/Width/Draft en metros
        f.first("Length", ignorenulls=True).alias("Length"),
        f.first("Width", ignorenulls=True).alias("Width"),
        f.first("Draft", ignorenulls=True).alias("Draft"),

        # Marcas de tiempo de la visita
        f.min(f.when(f.col("en_puerto"), f.col("BaseDateTime"))).alias("entrada_ts"),
        f.max(f.when(f.col("en_puerto"), f.col("BaseDateTime"))).alias("ultima_posicion_en_puerto_ts"),
        f.max(f.when(f.col("Flag_Salida"), f.col("BaseDateTime"))).alias("salida_ts"),

        # Calidad/confianza de la visita
        f.sum(f.col("en_puerto").cast("int")).alias("Total_Posiciones_En_Puerto"),
        f.min(f.when(f.col("en_puerto"), f.col("Hexagonos_Al_Centro_WPI"))).alias("Hexagonos_Min_Al_Centro_WPI"),

        # Atributos del puerto: solo de las filas donde en_puerto es True (en las de cierre es null)
        f.first(f.when(f.col("en_puerto"), f.col("wpi_id")), ignorenulls=True).alias("wpi_id"),
        f.first(f.when(f.col("en_puerto"), f.col("nombre_puerto")), ignorenulls=True).alias("nombre_puerto"),
        f.first(f.when(f.col("en_puerto"), f.col("pais")), ignorenulls=True).alias("pais"),
        f.first(f.when(f.col("en_puerto"), f.col("tamano_puerto")), ignorenulls=True).alias("tamano_puerto"),
    )
    .filter(f.col("entrada_ts").isNotNull())
    .withColumn("entrada_censurada", f.col("Visita_ID") == 0)
    .withColumn("salida_censurada", f.col("salida_ts").isNull())
    .withColumn("Fecha_Entrada", f.to_date("entrada_ts"))
    .withColumn("Hora_Entrada", f.hour("entrada_ts"))
    .withColumn("Fecha_Salida", f.to_date("salida_ts"))
    .withColumn("Hora_Salida", f.hour("salida_ts"))
    .withColumn("Duracion_Horas",
        (f.col("salida_ts").cast("long") - f.col("entrada_ts").cast("long")) / 3600
    )
    # Descripción legible del tipo de buque, reutilizando la tabla ya construida en 02_eda_silver.py
    .join(spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.vessel_types"), "VesselType", "left")
    .withColumn(
        "Visita_SK",
        f.sha2(f.concat_ws("|", f.col("MMSI_Real"), f.col("entrada_ts").cast("string")), 256)
    )
    # Reordenamiento final: llave y dimensiones contiguas primero, métricas de la fact al final.
    .select(
        "Visita_SK", "MMSI_Real", "Visita_ID",
        "MMSI", "VesselName", "VesselType", "Descripcion_Tipo", "MMSI_Anomalo",
        "Length", "Width", "Draft",
        "wpi_id", "nombre_puerto", "pais", "tamano_puerto",
        "Fecha_Entrada", "Hora_Entrada", "Fecha_Salida", "Hora_Salida",
        "entrada_ts", "ultima_posicion_en_puerto_ts", "salida_ts", "Duracion_Horas",
        "Total_Posiciones_En_Puerto", "Hexagonos_Min_Al_Centro_WPI",
        "entrada_censurada", "salida_censurada",
    )
)

display(df_visitas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plan de ejecución: join final con `vessel_types`
# MAGIC
# MAGIC `vessel_types` es la misma tabla chica (~100 filas) usada en `03_preguntas_negocio.py`; se
# MAGIC espera el mismo `BroadcastHashJoin` ya confirmado ahí. Sirve como control: si aquí no aparece,
# MAGIC algo en la construcción de `df_visitas` está rompiendo esa optimización.

# COMMAND ----------

df_visitas.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requisito 4: evidencia de almacenamiento óptimo
# MAGIC
# MAGIC **Compresión por agregación:** cuántas posiciones crudas representa cada visita — justifica
# MAGIC por qué vale la pena materializar esta tabla, más allá del formato o el layout.
# MAGIC
# MAGIC **Layout:** se escribe `gold.visitas_puerto` dos veces — sin `CLUSTER BY` y con
# MAGIC `CLUSTER BY (wpi_id, Fecha_Entrada)` — y se corre la misma consulta representativa del
# MAGIC propósito declarado (un puerto y una fecha concretos) contra ambas, midiendo archivos leídos.

# COMMAND ----------

total_posiciones = spark.read.table(f"{CATALOG}.{SOURCE_SCHEMA}.{SOURCE_TABLE_AIS}").count()
total_visitas = df_visitas.count()
print(f"Posiciones crudas en {SOURCE_TABLE_AIS}: {total_posiciones:,}")
print(f"Visitas a puerto detectadas:            {total_visitas:,}")
print(f"Factor de compresión:                   {total_posiciones / total_visitas:,.0f}x")

# COMMAND ----------

TABLA_SIN_CLUSTER = f"{DEST_TABLE_VISITAS}_sin_cluster"
TABLA_CON_CLUSTER = DEST_TABLE_VISITAS

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DEST_SCHEMA} COMMENT 'Capa Gold: hechos derivados para preguntas de negocio y comparación de layouts'")

# ──── Sin CLUSTER BY (línea base) ────
(
    df_visitas.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{DEST_SCHEMA}.{TABLA_SIN_CLUSTER}")
)

# ──── Con CLUSTER BY (wpi_id, Fecha_Entrada), alineado al propósito declarado ────
(
    df_visitas.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("wpi_id", "Fecha_Entrada")
    .saveAsTable(f"{CATALOG}.{DEST_SCHEMA}.{TABLA_CON_CLUSTER}")
)

print(f"Tablas creadas: {DEST_SCHEMA}.{TABLA_SIN_CLUSTER} y {DEST_SCHEMA}.{TABLA_CON_CLUSTER}")

# COMMAND ----------

# ──── Consulta representativa: el puerto y la fecha con más visitas ────
referencia = (
    spark.read.table(f"{CATALOG}.{DEST_SCHEMA}.{TABLA_CON_CLUSTER}")
    .groupBy("wpi_id", "nombre_puerto", "Fecha_Entrada")
    .count()
    .orderBy(f.desc("count"))
    .limit(1)
    .collect()
)[0]

WPI_REF = referencia["wpi_id"]
FECHA_REF = referencia["Fecha_Entrada"]
print(f"Puerto de referencia: {referencia['nombre_puerto']} (wpi_id={WPI_REF})")
print(f"Fecha de referencia: {FECHA_REF}")

def consulta_referencia(df):
    return df.filter((f.col("wpi_id") == WPI_REF) & (f.col("Fecha_Entrada") == FECHA_REF))

def archivos_leidos(tabla):
    # input_file_name() no está soportado sobre tablas gobernadas por Unity Catalog;
    # _metadata.file_path es el equivalente que sí funciona ahí.
    df = consulta_referencia(spark.read.table(tabla))
    n_filas = df.count()
    n_archivos = (
        df.select(f.col("_metadata.file_path").alias("_archivo"))
        .distinct()
        .count()
    )
    return n_filas, n_archivos

for tabla in [TABLA_SIN_CLUSTER, TABLA_CON_CLUSTER]:
    filas, archivos = archivos_leidos(f"{CATALOG}.{DEST_SCHEMA}.{tabla}")
    print(f"{tabla:30s} -> {filas:>4} filas, {archivos:>4} archivos distintos leídos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plan de ejecución sobre la tabla con `CLUSTER BY`
# MAGIC
# MAGIC Se revisa el plan físico de la consulta de referencia, buscando poda de archivos
# MAGIC (`PartitionFilters`/`OptionalDataFilters`) real, no solo el filtro lógico.

# COMMAND ----------

consulta_referencia(
    spark.read.table(f"{CATALOG}.{DEST_SCHEMA}.{TABLA_CON_CLUSTER}")
).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusión
# MAGIC
# MAGIC **Compresión:** 60 371 277 posiciones crudas en `silver.ais_cleaned` se resumen en 41 077
# MAGIC visitas a puerto — un factor de **1 470x**. Justifica por sí solo materializar esta tabla,
# MAGIC independientemente del formato o layout: sin ella, cualquier pregunta sobre tráfico portuario
# MAGIC tendría que reprocesar 60 millones de filas cada vez.
# MAGIC
# MAGIC **Layout:** para la consulta representativa (`wpi_id = 16010`, San Diego, `Fecha_Entrada =
# MAGIC 2023-06-01`, 335 filas en ambos casos):
# MAGIC
# MAGIC | Tabla | Archivos leídos |
# MAGIC |---|---|
# MAGIC | `visitas_puerto_sin_cluster` | 22 |
# MAGIC | `visitas_puerto` (`CLUSTER BY wpi_id, Fecha_Entrada`) | 1 |
# MAGIC
# MAGIC El plan físico de la tabla clusterizada confirma `DictionaryFilters: [(Fecha_Entrada =
# MAGIC 2023-06-01), (wpi_id = 16010)]` — poda real de archivos, no solo el filtro lógico. `CLUSTER BY
# MAGIC (wpi_id, Fecha_Entrada)` reduce los archivos leídos de 22 a 1 para el propósito declarado
# MAGIC (consulta diaria por puerto y fecha), confirmando la decisión de layout.
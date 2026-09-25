# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile("uniandes").serverless(True).getOrCreate()

import os

from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql import Window


# COMMAND ----------

CATALOG = 'oceanwatch'
RAW_SCHEMA = 'raw'
AIS_VOLUME = 'ais_files'
POSITIONS_PATH = 'positions'
ZIP_PATH = os.path.join(POSITIONS_PATH, 'zips')
CSV_PATH = os.path.join(POSITIONS_PATH, 'csvs')

VOLUME_BASE = os.path.join('/Volumes',CATALOG, RAW_SCHEMA, AIS_VOLUME)
CSV_DIR = os.path.join(VOLUME_BASE, CSV_PATH)
ZIPS_DIR = os.path.join(VOLUME_BASE, ZIP_PATH)

# COMMAND ----------

print(f"{VOLUME_BASE}")

# COMMAND ----------

ais_schema = StructType([
    StructField("MMSI", StringType(), True),
    StructField("BaseDateTime", TimestampType(), True),
    StructField("LAT", DoubleType(), True),
    StructField("LON", DoubleType(), True),
    StructField("SOG", DoubleType(), True),
    StructField("COG", DoubleType(), True),
    StructField("Heading", DoubleType(), True),
    StructField("VesselName", StringType(), True),
    StructField("IMO", StringType(), True),
    StructField("CallSign", StringType(), True),
    StructField("VesselType", IntegerType(), True), # Código numérico
    StructField("Status", IntegerType(), True),
    StructField("Length", DoubleType(), True),
    StructField("Width", DoubleType(), True),
    StructField("Draft", DoubleType(), True),
    StructField("Cargo", IntegerType(), True),
    StructField("TransceiverClass", StringType(), True)
])

# COMMAND ----------

df_ais = (
    spark.read
    .option("header", True)
    .schema(ais_schema)
    .csv(f"{CSV_DIR}/*.csv")
)

display(df_ais.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # Diccionario de Datos: Tráfico Marítimo (Sistema AIS)
# MAGIC
# MAGIC Este dataset contiene transmisiones continuas de los buques mercantes mediante el sistema AIS (Automatic Identification System). Cada fila representa un mensaje transmitido en un momento específico con la posición, velocidad, rumbo y características del buque.
# MAGIC
# MAGIC ### Identificadores del Buque
# MAGIC * **`MMSI`**: "Maritime Mobile Service Identity value", Identificador del buque. Es un número único de 9 dígitos que actúa como el identificador de la estación de radio del barco. *Tipo: Texto*
# MAGIC * **`VesselName`**: Nombre comercial del buque. *Tipo: Texto*
# MAGIC * **`IMO`**: "International Maritime Organization Vessel number", Número de la Organización Marítima Internacional. Es un número de identificación único que acompaña al barco durante toda su vida útil, incluso si cambia de nombre o de bandera. *Tipo: Texto*
# MAGIC * **`CallSign`**: Matrícula o identificación oficial para cualquier entidad que transmita señales de radiofrecuencia en territorio estadounidense asignada por la FCC. *Tipo: Texto*
# MAGIC
# MAGIC ### Datos Espacio-Temporales y de Navegación
# MAGIC * **`BaseDateTime`**: Fecha y hora exacta (Timestamp) en la que el buque emitió el mensaje. *Tipo: DateTime*
# MAGIC * **`LAT`**: Latitud de la posición geográfica del buque. *Tipo: Double*
# MAGIC * **`LON`**: Longitud de la posición geográfica del buque. *Tipo: Double*
# MAGIC * **`SOG`** (Speed Over Ground): Velocidad sobre el fondo, medida en nudos. Indica qué tan rápido se mueve el barco en términos absolutos de distancia recorrida. *Tipo: Float*
# MAGIC * **`COG`** (Course Over Ground): Rumbo sobre el fondo. Indica la dirección física real en la que avanza el barco. *Tipo: Float*
# MAGIC * **`Heading`**: Hacia dónde está apuntando la proa (la punta) del barco. Puede diferir del `COG` si el barco está siendo empujado lateralmente por vientos o corrientes marinas. *Tipo: Float*
# MAGIC * **`Status`**: Estado de navegación actual (ej. navegando a motor, fondeado, amarrado). *Tipo: Entero*
# MAGIC
# MAGIC ### Características Físicas y Clasificación
# MAGIC * **`VesselType`**: Código numérico que clasifica la categoría del buque. Estos códigos corresponden a las categorías del estándar AIS:
# MAGIC
# MAGIC  0: Default or not available (1–19 reserved)
# MAGIC
# MAGIC  20–29: Wing in Ground (WIG) craft, including general and hazardous categories
# MAGIC
# MAGIC  30–39: Fishing, towing, dredging, diving, military, sailing, and pleasure craft
# MAGIC
# MAGIC  40–49: High Speed Craft (HSC)
# MAGIC
# MAGIC  50–59: Special craft including pilot, search/rescue, tug, and law enforcement
# MAGIC
# MAGIC  60–69: Passenger vessels
# MAGIC
# MAGIC  70–79: Cargo vessels
# MAGIC
# MAGIC  80–89: Tankers *Tipo: Entero*
# MAGIC
# MAGIC * **`Length`**: Eslora. Es la longitud o largo total del buque en metros. *Tipo: Float*
# MAGIC * **`Width`**: Manga. Es el ancho total del buque en metros. *Tipo: Float*
# MAGIC * **`Draft`**: Calado. Es la profundidad de la parte del barco que va sumergida bajo el agua. *Tipo: Float*
# MAGIC * **`Cargo`**: Código que indica el tipo de carga o mercancía que transporta el barco. *Tipo: Texto*
# MAGIC * **`TransceiverClass`**: Clase del transmisor AIS (generalmente Clase A para grandes buques comerciales y Clase B para embarcaciones más pequeñas o de recreo). *Tipo: Texto*

# COMMAND ----------

# ──── 1. Verificar carga: Posiciones por día ────
display(
    df_ais
    .withColumn("Fecha", f.to_date("BaseDateTime"))
    .groupBy("Fecha")
    .count()
    .orderBy("Fecha")
)

# COMMAND ----------

# ──── Distribución Estadística de Velocidades por Tipo de Buque ────
display(
    df_ais
    .groupBy("VesselType")
    .agg(
        # Conteo para saber el peso de la categoría
        f.count("*").alias("Total_Mensajes"),

        # Velocidad Máxima absoluta (para ver los valores extremos)
        f.max("SOG").alias("Max_Absoluto"),

        # Percentiles usando aproximaciones estadísticas para evitar shuffles masivos[cite: 2]
        f.percentile_approx("SOG", 0.5, 10000).alias("Mediana_P50"),
        f.percentile_approx("SOG", 0.9, 10000).alias("Percentil_90"),
        f.percentile_approx("SOG", 0.99, 10000).alias("Percentil_99")
    )
    .orderBy(f.desc("Percentil_99"))
)

# COMMAND ----------

display(
    df_ais
    .agg(
        f.count("*").alias("Total_Registros"),

        # Buques únicos (MMSI es el identificador principal del buque)
        f.countDistinct("MMSI").alias("Buques_Unicos_Exactos"),

        # Posiciones fuera de rango terrestre (Latitud válida: -90 a 90, Longitud: -180 a 180)
        f.sum(f.when((f.col("LAT") < -90) | (f.col("LAT") > 90), 1).otherwise(0)).alias("Latitud_Invalida"),
        f.sum(f.when((f.col("LON") < -180) | (f.col("LON") > 180), 1).otherwise(0)).alias("Longitud_Invalida"),

        # Velocidades imposibles: SOG (Speed Over Ground) está en nudos.
        # Un buque mercante rara vez supera los 30-40 nudos. Incluso los botes de alta velocidad usualmente no superan los 50 nudos.  Usaremos > 60 como primera marca de anomalía.
        f.sum(f.when(f.col("SOG") > 60, 1).otherwise(0)).alias("Velocidades_Imposibles"),

        # Identificadores anómalos: El MMSI oficial de un transpondedor debe tener exactamente 9 dígitos
        f.sum(f.when(f.length(f.col("MMSI")) != 9, 1).otherwise(0)).alias("MMSI_Invalido")
    )
)

# COMMAND ----------

# Duplicados por (MMSI, BaseDateTime): mismo buque, mismo instante
display(
    df_ais
    .groupBy("MMSI", "BaseDateTime")
    .count()
    .filter("count > 1")
)

# COMMAND ----------

# =====================================================================
# FASE 1: LIMPIEZA BASE Y CREACIÓN DE ENTIDADES (df_silver_base)
# =====================================================================
df_silver_base = (
    df_ais
    # ─── A. LIMPIEZA FÍSICA Y DE HARDWARE ───
    .filter(f.col("SOG") != 102.3)
    .filter(f.col("SOG") <= 60)
    .filter(f.col("LAT").between(-90, 90))
    .filter(f.col("LON").between(-180, 180))
    .dropDuplicates(["MMSI", "BaseDateTime"])
    .withColumn("MMSI_Anomalo",
        f.when((f.length(f.col("MMSI")) != 9) | f.col("MMSI").isNull(), True)
        .otherwise(False)
    )

    # ─── B. ENRIQUECIMIENTO ESPACIAL (H3) ───
    .withColumn("H3_Macro", f.expr("h3_longlatash3(LON, LAT, 3)"))
    .withColumn("H3_Micro", f.expr("h3_longlatash3(LON, LAT, 7)"))

    # ─── C. CREACIÓN DEL ALIAS (Para aislar ventanas de cálculo) ───
    .withColumn("MMSI_Real", f.concat_ws("_", f.col("MMSI"), f.col("H3_Macro")))
)

# =====================================================================
# FASE 2: DEFINICIÓN DE VENTANAS LÓGICAS
# =====================================================================
w_viaje = Window.partitionBy("MMSI_Real").orderBy("BaseDateTime")

w_viaje_acum = (
    Window.partitionBy("MMSI_Real")
    .orderBy("BaseDateTime")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación de las fórmulas usadas en `df_silver_final`
# MAGIC
# MAGIC Antes de aplicar `st_distancesphere` y la resta de timestamps sobre los datos reales, se
# MAGIC verificaron ambas expresiones con casos de control:
# MAGIC
# MAGIC - **Distancia:** se calculó la distancia entre Bogotá (4.6, -74.0) y Nueva York (40.7, -74.0)
# MAGIC   con `st_distancesphere(st_point(prev_lon, prev_lat), st_point(LON, LAT)) / 1852`. Resultado:
# MAGIC   ~2 167 millas náuticas, consistente con la distancia real entre ambas ciudades (~4 014 km).
# MAGIC - **Tiempo:** se calculó `horas_transcurridas` entre `2023-06-01T00:01:58` y
# MAGIC   `2023-06-01T03:01:58` con `(BaseDateTime.cast("long") - prev_time.cast("long")) / 3600`.
# MAGIC   Resultado: 3.0 horas, como se esperaba de la diferencia exacta entre ambos timestamps.
# MAGIC
# MAGIC Con ambas fórmulas confirmadas, se aplican directamente sobre `df_silver_base` a continuación.
# MAGIC

# COMMAND ----------

# =====================================================================
# FASE 3: SEGMENTACIÓN, FILTRO DE VELOCIDAD Y MÉTRICAS (df_silver_final)
# =====================================================================
df_silver_final = (
    df_silver_base
    # ─── D. CURSORES TOPOLÓGICOS, FÍSICOS Y TEMPORALES ───
    .withColumn("prev_H3_Micro", f.lag("H3_Micro").over(w_viaje))
    .withColumn("prev_time", f.lag("BaseDateTime").over(w_viaje))
    .withColumn("prev_LAT", f.lag("LAT").over(w_viaje))
    .withColumn("prev_LON", f.lag("LON").over(w_viaje))

    # Cálculos relativos
    .withColumn("h3_distancia", f.expr("h3_distance(prev_H3_Micro, H3_Micro)"))
    .withColumn("horas_transcurridas",
        ((f.col("BaseDateTime").cast("long")) - (f.col("prev_time").cast("long"))) / 3600
    )

    # 1. Distancia bruta inicial (Haversine) en millas náuticas
    .withColumn("distancia_bruta_nm",
        f.when(f.col("prev_LAT").isNotNull(),
            f.expr("st_distancesphere(st_point(prev_LON, prev_LAT), st_point(LON, LAT))") / 1852
        ).otherwise(0.0)
    )

    # 2. Velocidad cinemática implícita (Millas Náuticas / Horas = Nudos)
    .withColumn("velocidad_implicita_nudos",
        f.when(f.col("horas_transcurridas") > 0,
               f.col("distancia_bruta_nm") / f.col("horas_transcurridas"))
        .otherwise(0.0)
    )

    # ─── E. REGLA DE SEGMENTACIÓN (LA BANDERA) ───
    # Marca corte si salta > 2 hexágonos micro, se detiene > 12h o supera 60 nudos físicos
    .withColumn("Flag_Nuevo_Viaje",
        f.when(f.col("prev_H3_Micro").isNull(), 0)
        .when(
            (f.col("h3_distancia") > 2) |
            (f.col("horas_transcurridas") > 12) |
            (f.col("velocidad_implicita_nudos") > 60),
            1
        )
        .otherwise(0)
    )

    # 3. Distancia final limpia (Se anula a 0.0 si hay salto/corte de viaje o velocidad > 60 nudos)
    .withColumn("distancia_segmento_nm",
        f.when(
            (f.col("Flag_Nuevo_Viaje") == 1) | (f.col("velocidad_implicita_nudos") > 60),
            0.0
        ).otherwise(f.col("distancia_bruta_nm"))
    )

    # ─── F. CONSOLIDACIÓN DE VIAJE (ID ACUMULATIVO) ───
    .withColumn("Trip_ID", f.sum("Flag_Nuevo_Viaje").over(w_viaje_acum))
)

# =====================================================================
# FASE 4: VISUALIZACIÓN DE CONTROL
# =====================================================================
display(df_silver_final.limit(100))

# COMMAND ----------

display(
    df_silver_final
    .filter(f.col("MMSI") == "211002010")
    .limit(150)
)

# COMMAND ----------


# 1. Aseguramos la columna de (Fecha) para la capa Silver
df_silver_final = df_silver_final.withColumn("Fecha", f.to_date("BaseDateTime"))

# 2. Forzamos acciones de conteo para comparar el antes y el después
total_bronce = df_ais.count()
total_plata = df_silver_final.count()
registros_eliminados = total_bronce - total_plata

# 3. Determinamos las columnas agregadas comparando los esquemas
columnas_bronce = set(df_ais.columns)
columnas_plata = set(df_silver_final.columns)
columnas_agregadas = columnas_plata - columnas_bronce

# 4. Reporte de cambios
print("="*50)
print("REPORTE DE LIMPIEZA - CAPA SILVER")
print("="*50)
print(f"Total de registros originales (Bronce): {total_bronce:,}")
print(f"Total de registros limpios (Plata):     {total_plata:,}")
print(f"Registros eliminados (anomalías/dups):  {registros_eliminados:,}")
print(f"Columnas derivadas agregadas:           {', '.join(columnas_agregadas)}")
print("="*50)

# COMMAND ----------

# ──── Materialización en Capa Plata con Liquid Clustering ────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver COMMENT 'Capa Plata: Datos AIS limpios y deduplicados'")

df_silver = (
    df_silver_final
    .drop("prev_H3_Micro", "prev_time", "prev_LAT", "prev_LON", "distancia_bruta_nm")
)

# 2. Escritura física en Delta Lake optimizada con Liquid Clustering
(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("MMSI", "BaseDateTime")
    .saveAsTable(f"{CATALOG}.silver.ais_cleaned")
)

print(f"Tabla {CATALOG}.silver.ais_cleaned actualizada y optimizada exitosamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla de referencia: World Port Index
# MAGIC
# MAGIC El CSV crudo del World Port Index (`raw/ais_files/reference/`, descargado en el notebook 01)
# MAGIC trae 109 columnas y cobertura mundial. Para el cruce con las celdas H3 del tráfico solo se
# MAGIC necesita la posición y el nombre de cada puerto, así que se aplica un ajuste pequeño: se
# MAGIC seleccionan y renombran las columnas relevantes, se tipan lat/lon como double y se descartan
# MAGIC filas sin coordenadas. El resultado queda como tabla fuente en la capa plata.

# COMMAND ----------

REFERENCE_DIR = os.path.join(VOLUME_BASE, 'reference')
WPI_PATH = os.path.join(REFERENCE_DIR, "world_port_index.csv")

df_wpi_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(WPI_PATH)
)

df_wpi = (
    df_wpi_raw
    .select(
        f.col("World Port Index Number").cast("int").alias("wpi_id"),
        f.col("Main Port Name").alias("nombre_puerto"),
        f.col("Country Code").alias("pais"),
        f.col("Latitude").cast("double").alias("lat"),
        f.col("Longitude").cast("double").alias("lon"),
        f.col("Harbor Size").alias("tamano_puerto"),
        f.col("Harbor Type").alias("tipo_puerto"),
    )
    .filter(f.col("lat").isNotNull() & f.col("lon").isNotNull())
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver COMMENT 'Capa Plata: Datos AIS limpios y deduplicados'")

(
    df_wpi.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.world_port_index")
)

print(f"Tabla {CATALOG}.silver.world_port_index creada: {df_wpi.count():,} puertos.")
display(df_wpi.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla de referencia: catálogo de tipos de buque
# MAGIC
# MAGIC El diccionario `catalogo_dict` recrea en código las categorías del estándar AIS/NAIS
# MAGIC (`VesselType`), tomadas de:
# MAGIC
# MAGIC - **NOAA MarineCadastre — VesselTypeCodes2018.pdf**
# MAGIC   https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf
# MAGIC
# MAGIC No se descarga el PDF al Volume porque no lo consume ningún paso del pipeline: solo se usó
# MAGIC para construir manualmente el mapeo de códigos a categorías. Se materializa como tabla en la
# MAGIC capa plata para que las preguntas de negocio la lean igual que `world_port_index`, en vez de
# MAGIC depender de una variable en memoria.

# COMMAND ----------

# ──── 1. Recreando el Diccionario Completo (AIS Ship Types)[cite: 5] ────
catalogo_dict = {
    0: "Not available",
    30: "Fishing", 31: "Towing", 32: "Towing exceeds 200m or 25m breadth",
    33: "Dredging or underwater ops", 34: "Diving ops", 35: "Military ops",
    36: "Sailing", 37: "Pleasure Craft", 50: "Pilot Vessel",
    51: "Search and Rescue vessel", 52: "Tug", 53: "Port Tender",
    54: "Anti-pollution equipment", 55: "Law Enforcement", 58: "Medical Transport",
    59: "Noncombatant ship"
}

# Rellenar rangos (familias de barcos)[cite: 5]
for i in range(1, 20): catalogo_dict[i] = "Reserved"
for i in range(20, 30): catalogo_dict[i] = f"Wing in ground (WIG) - Type {i}"
for i in range(38, 40): catalogo_dict[i] = "Reserved"
for i in range(40, 50): catalogo_dict[i] = f"High speed craft - Type {i}"
for i in range(56, 58): catalogo_dict[i] = "Spare Local Vessel"
for i in range(60, 70): catalogo_dict[i] = f"Passenger - Type {i}"
for i in range(70, 80): catalogo_dict[i] = f"Cargo - Type {i}"
for i in range(80, 90): catalogo_dict[i] = f"Tanker - Type {i}"
for i in range(90, 100): catalogo_dict[i] = f"Other Type - Type {i}"

data_catalogo_completo = [(k, v) for k, v in catalogo_dict.items()]
df_catalogo_completo = spark.createDataFrame(data_catalogo_completo, ["VesselType", "Descripcion_Tipo"])

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver COMMENT 'Capa Plata: Datos AIS limpios y deduplicados'")

(
    df_catalogo_completo.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.vessel_types")
)

print(f"Tabla {CATALOG}.silver.vessel_types creada: {df_catalogo_completo.count():,} tipos.")
display(df_catalogo_completo.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Pregunta 3

# COMMAND ----------

# =====================================================================
# CAPA GOLD: TOP 10 BUQUES POR DISTANCIA SEMANAL (CONSOLIDADO POR MMSI)
# =====================================================================

df_top_viajeros = (
    spark.read.table(f"{CATALOG}.silver.ais_cleaned")

    # 1. Excluimos MMSIs defectuosos o genéricos
    .filter(f.col("MMSI_Anomalo") == False)

    # 2. Agrupamos por la entidad física (MMSI) para unir todos los H3_Macro que cruzó
    .groupBy("MMSI")
    .agg(
        f.first("VesselName", ignorenulls=True).alias("VesselName"),
        f.first("VesselType", ignorenulls=True).alias("VesselType"),
        f.round(f.sum("distancia_segmento_nm"), 2).alias("Distancia_Total_Millas"),
        f.round(
            f.avg(f.when(f.col("velocidad_implicita_nudos") > 0.5, f.col("velocidad_implicita_nudos"))),
            2
        ).alias("Velocidad_Crucero_Prom_Nudos"),
        f.countDistinct("H3_Macro").alias("Hexagonos_Macro_Atravesados"),
        f.count("*").alias("Total_Tramos_Calculados")
    )

    # 3. Enriquecimiento con el catálogo de tipos de buque
    .join(df_catalogo_completo, "VesselType", "left")

    # 4. Ordenamiento y selección del Top 10
    .orderBy(f.desc("Distancia_Total_Millas"))
    .limit(10)
    .select(
        "MMSI",
        "VesselName",
        "Descripcion_Tipo",
        "Distancia_Total_Millas",
        "Velocidad_Crucero_Prom_Nudos",
        "Hexagonos_Macro_Atravesados",
        "Total_Tramos_Calculados"
    )
)

print("Top 10 Buques con mayor distancia recorrida (Con Candado Cinemático y Reconexión H3):")
display(df_top_viajeros)

# COMMAND ----------

# MMSI del "Pleasure Craft" con distancia irreal
mmsi_sospechoso = "367638030"

df_diagnostico_gps = (
    # Leemos directamente de la tabla en tu capa Silver
    spark.read.table(f"{CATALOG}.silver.ais_cleaned")
    .filter(f.col("MMSI") == mmsi_sospechoso)
    .select(
        "MMSI_Real",
        "MMSI",
        "IMO",
        "heading",
        "status",
        "VesselType",
        "VesselName",
        "BaseDateTime",
        "LAT",
        "LON",
        "distancia_segmento_nm",
        "SOG"
    )
    .orderBy("BaseDateTime")
    .limit(10000)
)

display(df_diagnostico_gps)
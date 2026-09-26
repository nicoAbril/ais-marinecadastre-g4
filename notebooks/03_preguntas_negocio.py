# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Preguntas de negocio (AIS MarineCadastre)
# MAGIC
# MAGIC Responde las preguntas de negocio del punto 3 del enunciado, leyendo directamente las
# MAGIC tablas de la capa plata construidas en `02_eda_silver.py`:
# MAGIC `silver.ais_cleaned`, `silver.world_port_index` y `silver.vessel_types`.
# MAGIC Este notebook no reconstruye esas tablas, solo las consulta.

# COMMAND ----------

try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile("uniandes").serverless(True).getOrCreate()

from pyspark.sql import functions as f
from pyspark.sql import Window

CATALOG = 'oceanwatch'

# COMMAND ----------

# MAGIC %md
# MAGIC Preguntas iniciales

# COMMAND ----------

# ──── 0. Filtro de identidad válida ────
# MMSI_Anomalo == False excluye identificadores nulos o fuera del estándar de 9 dígitos, para no
# colapsar cientos de buques distintos bajo un mismo código genérico (000000000, etc.).
# Se cuenta MMSI (no MMSI_Real): MMSI_Real aísla ventanas por región (H3_Macro) y un mismo buque en
# navegación continua cruza varias regiones en un día, lo que sobrecontaría cada buque en movimiento.
ais_validos = (
    spark.read.table(f"{CATALOG}.silver.ais_cleaned")
    .filter(f.col("MMSI_Anomalo") == False)
)

# ──── Método 1: Conteo Exacto ────
df_exacto = (
    ais_validos
    .groupBy("Fecha")
    .agg(f.countDistinct("MMSI").alias("Conteo_Exacto"))
    .orderBy("Fecha")
)

print("Calculando conteo exacto...")
display(df_exacto)

print("\n--- Plan Físico (Exacto) ---")
df_exacto.explain()

# COMMAND ----------

# ──── Método 2: Conteo Aproximado (Sketches) ────
df_aprox = (
    ais_validos
    .groupBy("Fecha")
    # rsd = 0.1 indica un error relativo máximo tolerado del 10%
    .agg(f.approx_count_distinct("MMSI", 0.1).alias("Conteo_Aproximado_10pct"))
    .orderBy("Fecha")
)

print("Calculando conteo aproximado...")
display(df_aprox)

print("\n--- Plan Físico (Aproximado) ---")
df_aprox.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Justificación: conteo exacto vs. aproximado
# MAGIC
# MAGIC Se cuenta sobre `MMSI` (no `MMSI_Real`) y con `MMSI_Anomalo == False`:
# MAGIC - **`MMSI` y no `MMSI_Real`:** `MMSI_Real` aísla ventanas de cálculo por región (`H3_Macro`,
# MAGIC   ~60 km), y un buque en navegación continua cruza varias regiones en un solo día. Contar
# MAGIC   `MMSI_Real` multiplicaría cada buque en movimiento entre sus regiones, sobrecontando la flota.
# MAGIC - **`MMSI_Anomalo == False`:** excluye identificadores nulos o fuera del estándar de 9 dígitos,
# MAGIC   para no colapsar cientos de buques distintos con transpondedores mal configurados bajo un
# MAGIC   mismo código genérico (`000000000`, etc.), lo que subestimaría el tamaño real de la flota.
# MAGIC
# MAGIC El plan del conteo exacto agrega con `count(distinct MMSI)`, que requiere materializar todos
# MAGIC los valores distintos por partición antes de agregarlos (shuffle completo por grupo). El plan
# MAGIC aproximado usa `HyperLogLog++` (`approx_count_distinct`), que agrega un sketch de tamaño fijo
# MAGIC por partición, sin necesitar los valores individuales en el shuffle.
# MAGIC
# MAGIC Para 7 días de datos el exacto es viable y preferible, porque la exactitud importa a esta
# MAGIC escala y el costo adicional es bajo. En producción, con volúmenes mucho mayores (meses o años
# MAGIC de AIS), se preferiría el aproximado para evitar el shuffle completo, aceptando un margen de
# MAGIC error a cambio de un plan más liviano.
# MAGIC
# MAGIC #### Evidencia empírica
# MAGIC
# MAGIC Comparando exacto vs. aproximado (`rsd=0.1`) sobre `MMSI` con `MMSI_Anomalo == False` para los
# MAGIC 7 días: el error relativo osciló entre **+1.5% y +13.2%**, con un promedio de **~8%**. Es una
# MAGIC cifra indicativa, no un número fijo: `approx_count_distinct` es un estimador aleatorio, así que
# MAGIC una nueva ejecución (u otro rango de fechas) puede dar errores distintos día a día, aunque del
# MAGIC mismo orden de magnitud.
# MAGIC
# MAGIC El error promedio observado (~8%) es del orden del rsd nominal (10%), lo esperado para este
# MAGIC estimador. Confirma la misma conclusión: a esta escala (7 días, decenas de miles de buques
# MAGIC únicos por día) el conteo exacto es viable y preferible, y el aproximado con este rsd es
# MAGIC demasiado impreciso para un número que alguien vaya a citar como cifra de negocio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análisis de Tráfico por Categoría de Embarcación
# MAGIC
# MAGIC > **Pregunta de negocio:** ¿Qué tipos de buque generan más tráfico? Top 10 por número de
# MAGIC > posiciones, con velocidad media por tipo (usando el catálogo de tipos).
# MAGIC
# MAGIC Para responder midiendo la ocupación real del espacio marítimo y el régimen de velocidad por
# MAGIC categoría, el bloque ejecuta una estrategia en tres pasos:
# MAGIC
# MAGIC 1. **Catálogo dimensional en tabla de referencia (`silver.vessel_types`):** el protocolo AIS
# MAGIC    transmite el tipo de embarcación como un código numérico (`VesselType`). Para traducirlo a
# MAGIC    categorías legibles se usa el diccionario del estándar ITU (códigos específicos como `30`
# MAGIC    para pesca o `52` para remolcadores, y familias por rangos decenales como `70-79` para
# MAGIC    carga), materializado como tabla en la capa plata.
# MAGIC 2. **Agregación de volumen y velocidad sin exclusión de identidad:** se agrupa `silver.ais_cleaned`
# MAGIC    directamente por `VesselType` para calcular el volumen de tráfico (`count("*")`) y la
# MAGIC    velocidad media (`avg("SOG")`). A diferencia de los rankings por buque individual, **aquí no
# MAGIC    se filtra `MMSI_Anomalo == False`**: el objetivo es cuantificar la densidad total de
# MAGIC    transmisiones y el comportamiento cinemático por categoría, y un barco con el identificador
# MAGIC    mal configurado sigue representando una embarcación física real cuya posición, tipo y
# MAGIC    velocidad (`SOG` ya saneado) son válidos.
# MAGIC 3. **Enriquecimiento tardío y evidencia del plan (`.explain()`):** el cruce con el catálogo se
# MAGIC    hace **después** del `groupBy`, lo que reduce la cardinalidad antes del join (de millones de
# MAGIC    eventos a menos de 100 filas). El broadcast se fuerza explícitamente (`f.broadcast(...)`), en
# MAGIC    vez de dejarlo a la heurística automática de Catalyst, para no depender de que el tamaño de
# MAGIC    `vessel_types` siga por debajo del umbral de auto-broadcast si la tabla crece más adelante.
# MAGIC    El `.orderBy().limit(10)` se resuelve con `PhotonTopK`, sin ordenar toda la agregación.

# COMMAND ----------

# ──── 1. Catálogo de tipos de buque (tabla de referencia) ────
df_catalogo_completo = spark.read.table(f"{CATALOG}.silver.vessel_types")

# ──── 2. Procesamiento Analítico (Aprovechando Liquid Clustering) ────
df_ais_silver = spark.read.table(f"{CATALOG}.silver.ais_cleaned")

df_top_tipos_completo = (
    df_ais_silver
    .groupBy("VesselType")
    .agg(
        f.count("*").alias("Total_Posiciones"),
        f.round(f.avg("SOG"), 2).alias("Velocidad_Media_Nudos")
    )
    # Cruce con el catálogo, broadcast explícito (no se deja a la heurística automática de Spark)
    .join(f.broadcast(df_catalogo_completo), "VesselType", "left")
    .orderBy(f.desc("Total_Posiciones"))
    .limit(10)
    .select("VesselType", "Descripcion_Tipo", "Total_Posiciones", "Velocidad_Media_Nudos")
)

print("Top 10 Tráfico por Tipo de Buque:")
display(df_top_tipos_completo)

# ──── 3. Evidencia Técnica ────
print("\n--- Plan de Ejecución ---")
df_top_tipos_completo.explain()

# El plan confirma dos decisiones tomadas al escribir la consulta:
# - PhotonBroadcastHashJoin (BuildRight): confirma que Spark difundió vessel_types (tabla chica,
#   ~100 filas) a los executors en vez de hacer shuffle de ais_cleaned (tabla grande), como se
#   esperaba en el comentario del join.
# - PhotonTopK sobre el orderBy + limit(10): el motor no ordena toda la agregación por VesselType,
#   solo mantiene los 10 mayores durante el shuffle, en vez de un sort completo seguido de un corte.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ranking de Embarcaciones con Mayor Distancia Recorrida
# MAGIC
# MAGIC > **Pregunta de negocio:** ¿Qué 10 buques recorrieron más distancia durante la semana? (orden
# MAGIC > por buque y tiempo, distancia entre posiciones consecutivas con haversine).
# MAGIC
# MAGIC El ordenamiento cronológico por embarcación y el cálculo de distancias esféricas entre
# MAGIC posiciones consecutivas ya se precalcularon en la capa Silver. Sobre esa base, este bloque
# MAGIC construye el ranking final en cuatro pasos:
# MAGIC
# MAGIC 1. **Filtro de identidad válida (`MMSI_Anomalo == False`):** a diferencia de la pregunta
# MAGIC    anterior, aquí es obligatorio excluir los identificadores nulos o fuera del estándar de 9
# MAGIC    dígitos, porque el objetivo es identificar embarcaciones individuales. Si no se filtraran,
# MAGIC    cientos de barcos que transmiten un mismo código genérico o por defecto (ej. `000000000`) se
# MAGIC    sumarían como si fueran un solo buque, ganando falsamente el primer lugar del ranking.
# MAGIC 2. **Reconexión espacial por buque (`groupBy("MMSI")`):** en la capa Silver las trayectorias se
# MAGIC    dividieron por regiones (`MMSI_Real`) para aislar barcos clonados en distintos mares. Como
# MAGIC    los saltos irreales ya fueron anulados a `0.0` (el candado cinemático), aquí es seguro
# MAGIC    agrupar de nuevo por el `MMSI` original para unir todas las regiones (`H3_Macro`) que un
# MAGIC    barco legítimo cruzó en la semana. Se usa `f.first(..., ignorenulls=True)` para recuperar el
# MAGIC    nombre y tipo de buque sin importar que algún reporte llegara vacío.
# MAGIC 3. **Consolidación de métricas de viaje y velocidad crucero:**
# MAGIC    - `Distancia_Total_Millas` suma `distancia_segmento_nm` (no la distancia bruta), así solo se
# MAGIC      suman millas de navegación continua y válida, ignorando cortes de viaje o saltos de GPS.
# MAGIC    - `Velocidad_Crucero_Prom_Nudos` promedia la velocidad implícita solo en tramos mayores a
# MAGIC      **0.5 nudos**, para que el promedio refleje la marcha real sin verse castigado por el
# MAGIC      tiempo que el barco estuvo fondeado, atracado o a la deriva por la marea.
# MAGIC    - `Hexagonos_Macro_Atravesados` y `Total_Tramos_Calculados` dan contexto sobre el alcance
# MAGIC      geográfico y la solidez de la muestra detrás de cada buque.
# MAGIC 4. **Orden, corte a Top 10 y enriquecimiento en ese orden:** primero se ordena de mayor a menor
# MAGIC    distancia y se corta a los 10 primeros; el cruce con `vessel_types` (`left join`, para que
# MAGIC    ningún barco del Top 10 se pierda por no haber reportado su categoría) se hace **después**
# MAGIC    del corte, no antes. Como es un `left join` que no filtra filas, el resultado es idéntico a
# MAGIC    hacerlo antes, pero el join solo procesa 10 filas en vez de todos los buques agrupados por
# MAGIC    `MMSI`. El broadcast también se fuerza explícitamente con `f.broadcast(...)`.

# COMMAND ----------

# =====================================================================
# PREGUNTA 3c: TOP 10 BUQUES POR DISTANCIA RECORRIDA EN LA SEMANA
# =====================================================================

df_catalogo_completo = spark.read.table(f"{CATALOG}.silver.vessel_types")

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

    # 3. Ordenamiento y corte al Top 10 antes del join: reduce el cruce a 10 filas en vez de
    # ejecutarlo sobre todos los buques agrupados (left join, no filtra filas: el resultado final
    # no cambia, solo se hace más barato)
    .orderBy(f.desc("Distancia_Total_Millas"))
    .limit(10)

    # 4. Enriquecimiento con el catálogo de tipos de buque, broadcast explícito
    .join(f.broadcast(df_catalogo_completo), "VesselType", "left")
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

print("\n--- Plan de Ejecución ---")
df_top_viajeros.explain()

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

# COMMAND ----------

# MAGIC %md
# MAGIC Pregunta 4
# MAGIC
# MAGIC > **Pregunta de negocio:** ¿Dónde se concentra el tráfico? Top 10 celdas de una grilla espacial
# MAGIC > (H3 resolución 8) por número de posiciones, y cuáles de esas celdas corresponden a puertos
# MAGIC > del World Port Index.
# MAGIC
# MAGIC 1. **Top 10 celdas de alta densidad:** se proyecta cada posición en H3 resolución 8
# MAGIC    (hexágonos de ~460 m de arista, escala de muelle/canal) y se agrega por celda. No se
# MAGIC    excluye `MMSI_Anomalo`, porque el objetivo es medir la saturación física del espacio, no
# MAGIC    identificar buques individuales. El `.limit(10)` se aplica antes del cruce con puertos para
# MAGIC    no expandir innecesariamente un conjunto que ya se va a descartar.
# MAGIC 2. **Expansión de puertos con `h3_kring`:** cada puerto del WPI es un solo punto, pero un
# MAGIC    buque atraca y fondea en un área de varios km alrededor. Se expande cada puerto a un anillo
# MAGIC    de radio 5 (`h3_kring`, ~4 km) para que las celdas de tráfico cercanas hagan match aunque no
# MAGIC    caigan exactamente en el hexágono del punto oficial.
# MAGIC 3. **Desambiguación:** cuando dos puertos vecinos comparten celdas por el traslape de sus
# MAGIC    anillos, se usa `row_number()` particionado por celda y ordenado por distancia en hexágonos,
# MAGIC    para que cada celda quede asociada solo a su puerto más cercano.
# MAGIC 4. **Cruce final:** `left join` por igualdad de `H3_Res8`, convirtiendo el problema geográfico
# MAGIC    en un cruce de texto. Si no hay match, la celda se clasifica como tráfico no catalogado.

# COMMAND ----------

# =====================================================================
# TOP 10 CELDAS H3 (RESOLUCIÓN 8) POR VOLUMEN DE POSICIONES
# =====================================================================
df_top10_celdas = (
    spark.read.table(f"{CATALOG}.silver.ais_cleaned")
    .withColumn("H3_Res8", f.expr("h3_longlatash3(LON, LAT, 8)"))
    .groupBy("H3_Res8")
    .agg(
        f.count("*").alias("Total_Posiciones"),
        f.countDistinct("MMSI").alias("Buques_Unicos"),
        f.round(f.avg("SOG"), 2).alias("Velocidad_Prom_Nudos"),
        f.round(f.avg("LAT"), 5).alias("Centroide_LAT"),
        f.round(f.avg("LON"), 5).alias("Centroide_LON")
    )
    .orderBy(f.desc("Total_Posiciones"))
    .limit(10)  # Reducción temprana a 10 filas antes del join
)

# =====================================================================
# EXPANSIÓN TOPOLÓGICA DEL WORLD PORT INDEX (silver.world_port_index)
# =====================================================================
df_wpi_expandido = (
    spark.read.table(f"{CATALOG}.silver.world_port_index")
    .filter(f.col("lat").isNotNull() & f.col("lon").isNotNull())

    # 1. Hexágono central de la coordenada oficial del puerto
    .withColumn("H3_Puerto_Centro", f.expr("h3_longlatash3(lon, lat, 8)"))

    # 2. Anillo k=5 (~4 km de cobertura portuaria para abarcar muelles y terminales)
    .withColumn("H3_Res8", f.expr("explode(h3_kring(H3_Puerto_Centro, 5))"))

    # 3. Distancia en número de hexágonos desde el muelle hasta la coordenada oficial
    .withColumn("Hexagonos_Al_Centro_WPI", f.expr("h3_distance(H3_Puerto_Centro, H3_Res8)"))
)

# =====================================================================
# DESAMBIGUACIÓN (1 CELDA H3 = 1 ÚNICO PUERTO MÁS CERCANO)
# =====================================================================
w_desambiguacion = Window.partitionBy("H3_Res8").orderBy("Hexagonos_Al_Centro_WPI", "wpi_id")

df_wpi_unico = (
    df_wpi_expandido
    .withColumn("rn", f.row_number().over(w_desambiguacion))
    .filter(f.col("rn") == 1)
    .drop("rn")
)

# =====================================================================
# CRUCE FINAL Y CLASIFICACIÓN DE CORRESPONDENCIA CON PUERTOS WPI
# =====================================================================
df_concentracion_trafico = (
    df_top10_celdas
    .join(df_wpi_unico, on="H3_Res8", how="left")
    .withColumn("Corresponde_Puerto_WPI",
        f.when(f.col("nombre_puerto").isNotNull(), f.lit("Sí"))
        .otherwise(f.lit("No (Zona no catalogada en WPI)"))
    )
    .select(
        "H3_Res8",
        "Total_Posiciones",
        "Buques_Unicos",
        "Velocidad_Prom_Nudos",
        "Centroide_LAT",
        "Centroide_LON",
        "Corresponde_Puerto_WPI",
        f.coalesce(f.col("nombre_puerto"), f.lit("N/A")).alias("Puerto_WPI"),
        f.coalesce(f.col("pais"), f.lit("N/A")).alias("Pais"),
        f.coalesce(f.col("tamano_puerto"), f.lit("N/A")).alias("Tamano_Puerto"),
        "Hexagonos_Al_Centro_WPI"
    )
    .orderBy(f.desc("Total_Posiciones"))
)

print("Top 10 Celdas H3 (Resolución 8) por densidad de tráfico y su cruce con World Port Index:")
display(df_concentracion_trafico)

# COMMAND ----------

# MAGIC %md
# MAGIC Pregunta 5
# MAGIC
# MAGIC > **Pregunta de negocio:** ¿Qué proporción de los buques de la semana transmitió los 7 días?
# MAGIC > ¿Dónde están los "visitantes de un solo día"?
# MAGIC
# MAGIC 1. **Perfil semanal por buque:** se filtra `MMSI_Anomalo == False` (los códigos genéricos
# MAGIC    sumarían sus días de navegación juntos, creando falsos buques de 7 días) y se agrupa por
# MAGIC    `MMSI`, calculando cuántos días distintos transmitió cada uno.
# MAGIC 2. **Proporción sobre la flota total:** se agrupa por `Dias_Activos` (1 a 7) y se usa una
# MAGIC    ventana global (`Window.partitionBy()`, sin partición real) para sumar el total de la flota
# MAGIC    semanal en una sola pasada, sin una segunda consulta aparte.
# MAGIC 3. **Cobertura portuaria en resolución 7:** los puertos del WPI se proyectan en H3 resolución 7
# MAGIC    (~1.2 km) y se expanden con un anillo de radio 3, para dar contexto geográfico a las celdas
# MAGIC    `H3_Micro` donde aparecen los buques ocasionales. Se desambigua igual que en la pregunta 4.
# MAGIC 4. **Visitantes de un solo día:** se filtran los buques con `Dias_Activos == 1`, se agrupan por
# MAGIC    su celda `H3_Micro` y se cruzan con los puertos expandidos, para ver si esos buques de paso
# MAGIC    se concentran en puertos oficiales o en zonas costeras/fronterizas sin catalogar.

# COMMAND ----------

# =====================================================================
# RESUMEN BASE POR BUQUE
# =====================================================================
df_dias_por_buque = (
    spark.read.table(f"{CATALOG}.silver.ais_cleaned")
    .filter(f.col("MMSI_Anomalo") == False)
    .groupBy("MMSI")
    .agg(
        f.countDistinct("Fecha").alias("Dias_Activos"),
        f.first("VesselName", ignorenulls=True).alias("VesselName"),
        f.first("H3_Micro", ignorenulls=True).alias("H3_Micro"),
        f.round(f.avg("LAT"), 4).alias("LAT_Prom"),
        f.round(f.avg("LON"), 4).alias("LON_Prom")
    )
)

# =====================================================================
# ¿QUÉ PROPORCIÓN TRANSMITIÓ LOS 7 DÍAS?
# =====================================================================
# Window.partitionBy() sin columnas mueve todas las filas a una sola partición (Spark lo advierte
# con un UserWarning: "No Partition Defined for Window operation"). Aquí es aceptable a propósito:
# df_dias_por_buque ya viene agregado a nivel de MMSI (~31 700 filas para 7 días), así que sumar el
# total de la flota en una sola partición es liviano. Con muchos más días o buques, esta ventana
# global se volvería un cuello de botella y convendría calcular el total con una agregación aparte
# (un solo count()) en vez de una ventana sin partición.
w_total = Window.partitionBy()

df_proporcion = (
    df_dias_por_buque
    .groupBy("Dias_Activos")
    .agg(f.count("MMSI").alias("Total_Buques"))
    .withColumn("Total_Flota_Semanal", f.sum("Total_Buques").over(w_total))
    .withColumn("Proporcion_Pct", f.round((f.col("Total_Buques") / f.col("Total_Flota_Semanal")) * 100, 2))
    .orderBy(f.desc("Dias_Activos"))
)

print("Parte A: Proporción de buques según días transmitidos (7 días vs. resto de la semana):")
display(df_proporcion)

# =====================================================================
# ¿DÓNDE ESTÁN LOS "VISITANTES DE UN SOLO DÍA"?
# =====================================================================
# Cobertura portuaria en H3_Micro (resolución 7, anillo k=3) para darle nombre a cada zona
df_puertos_res7 = (
    spark.read.table(f"{CATALOG}.silver.world_port_index")
    .filter(f.col("lat").isNotNull() & f.col("lon").isNotNull())
    .withColumn("H3_Puerto", f.expr("h3_longlatash3(lon, lat, 7)"))
    .withColumn("H3_Micro", f.expr("explode(h3_kring(H3_Puerto, 3))"))
    .withColumn("Dist_Hex", f.expr("h3_distance(H3_Puerto, H3_Micro)"))
)

w_puerto = Window.partitionBy("H3_Micro").orderBy("Dist_Hex")

df_puertos_res7 = (
    df_puertos_res7
    .withColumn("rn", f.row_number().over(w_puerto))
    .filter(f.col("rn") == 1)
    .select("H3_Micro", "nombre_puerto", "pais")
)

# Filtramos los de 1 solo día y agrupamos directamente por su celda H3_Micro existente
df_visitantes_1dia = (
    df_dias_por_buque
    .filter(f.col("Dias_Activos") == 1)
    .groupBy("H3_Micro")
    .agg(
        f.count("MMSI").alias("Buques_Un_Solo_Dia"),
        f.round(f.avg("LAT_Prom"), 4).alias("LAT_Centro"),
        f.round(f.avg("LON_Prom"), 4).alias("LON_Centro"),
        f.slice(f.collect_set("VesselName"), 1, 3).alias("Ejemplos_Buques")
    )
    .orderBy(f.desc("Buques_Un_Solo_Dia"))
    .limit(10)
    .join(df_puertos_res7, on="H3_Micro", how="left")
    .select(
        "H3_Micro",
        "Buques_Un_Solo_Dia",
        f.coalesce("nombre_puerto", f.lit("Zona costera / No catalogada en WPI")).alias("Puerto_Cercano"),
        f.coalesce("pais", f.lit("-")).alias("Pais"),
        "LAT_Centro",
        "LON_Centro",
        "Ejemplos_Buques"
    )
    .orderBy(f.desc("Buques_Un_Solo_Dia"))
)

print("Parte B: Top 10 zonas donde se concentran los 'visitantes de un solo día':")
display(df_visitantes_1dia)
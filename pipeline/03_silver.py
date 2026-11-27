# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 · Plata: compuertas de calidad, cuarentena y deduplicación idempotente
# MAGIC
# MAGIC **OceanWatch Analytics · Entrega 2** · Tarea 4 del job · Mapa completo del pipeline en el
# MAGIC [README](../README.md#entrega-2--pipeline-en-streaming).
# MAGIC
# MAGIC ## Propósito
# MAGIC
# MAGIC Convertir el registro de llegadas (bronce, todo texto, con repetidos) en posiciones
# MAGIC **confiables y únicas**. Cada fila de bronce termina en exactamente uno de tres lugares:
# MAGIC
# MAGIC | Destino | Qué filas | Cómo se escribe |
# MAGIC |---|---|---|
# MAGIC | `silver.ais_posiciones` | Pasan todas las compuertas; una por llave `(MMSI, H3_Macro, BaseDateTime)` | `MERGE` con ganador determinístico |
# MAGIC | `silver.ais_cuarentena` | Fallan al menos una regla dura; valores originales como texto | `MERGE` por `record_hash` |
# MAGIC | *(descartadas)* | Copias de una llave que ya tiene ganador | Se cuentan en `silver.calidad_metricas`; siguen en bronce |
# MAGIC
# MAGIC **Un registro no llega a oro si no pasa las compuertas:** oro solo lee `ais_posiciones`.
# MAGIC
# MAGIC ## Entradas y salidas
# MAGIC
# MAGIC | | Objeto | Detalle |
# MAGIC |---|---|---|
# MAGIC | **Entrada** | `bronze.ais_posiciones_raw` | Stream de Delta (lee solo lo nuevo de bronce) |
# MAGIC | **Entrada** | `silver.dim_h3_tierra_firme` | Máscara de la regla `en_tierra` |
# MAGIC | **Salida** | `silver.ais_posiciones` | Posiciones válidas, tipadas, deduplicadas |
# MAGIC | **Salida** | `silver.ais_cuarentena` | Filas inválidas con `motivo_principal` y `motivos` |
# MAGIC | **Salida** | `silver.calidad_metricas` | Métricas de calidad por fecha de archivo (formato largo) |
# MAGIC | **Estado** | `ops/checkpoints/silver` | Qué versiones de bronce ya se procesaron |
# MAGIC
# MAGIC **Parámetros:** `catalogo`; `reprocesar_desde_cero` (`false` por defecto; `true` borra el
# MAGIC checkpoint de plata para demostrar que reprocesar todo bronce no cambia plata).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decisiones
# MAGIC
# MAGIC **Compuertas.** Medidas sobre el 2023-06-01 (8 808 904 filas) antes de fijarlas: MMSI distinto
# MAGIC de 9 dígitos 6 911; timestamps y coordenadas inválidas 0; `SOG = 102.3` 21 946; `SOG > 60` 83;
# MAGIC `COG = 360` 1 490 036 (16,9 %); `Heading = 511` 4 871 561 (55,3 %); VesselType desconocido 765.
# MAGIC
# MAGIC | Prioridad | Regla dura → cuarentena | Condición |
# MAGIC |---|---|---|
# MAGIC | 1 | `timestamp_invalido` | `BaseDateTime` nulo o ilegible |
# MAGIC | 2 | `mmsi_invalido` | nulo o distinto de 9 dígitos (*"identificadores bien formados"*) |
# MAGIC | 3 | `coordenada_nula` | `LAT` o `LON` nulos o no numéricos |
# MAGIC | 4 | `coordenada_fuera_de_rango` | `LAT ∉ [-90, 90]` o `LON ∉ [-180, 180]` |
# MAGIC | 5 | `sog_implausible` | `SOG > 60` o `< 0`, distinto de 102.3 (*"velocidades plausibles"*) |
# MAGIC | 6 | `en_tierra` | `H3_Micro` en `dim_h3_tierra_firme` (*"en el agua"*) |
# MAGIC
# MAGIC `motivos` guarda **todas** las reglas que falla una fila; `motivo_principal` es la primera
# MAGIC en este orden. Las reglas suaves no sacan la fila de plata: anulan el valor del sensor y
# MAGIC dejan una bandera (no se descarta una posición válida por un sensor de velocidad malo):
# MAGIC
# MAGIC | Bandera | Condición | Tratamiento |
# MAGIC |---|---|---|
# MAGIC | `sog_no_disponible` | `SOG = 102.3` (código AIS) | `SOG` → nulo |
# MAGIC | `cog_no_disponible` | `COG = 360` | `COG` → nulo |
# MAGIC | `heading_no_disponible` | `Heading = 511` | `Heading` → nulo |
# MAGIC | `vessel_type_desconocido` | código fuera de 0-99 | se conserva |
# MAGIC
# MAGIC La velocidad *implícita* (salto entre dos puntos) no se evalúa aquí: es una propiedad de la
# MAGIC relación entre dos filas y depende de qué puntos hayan llegado; va en oro.
# MAGIC
# MAGIC **Por qué SOG > 60 nudos es regla dura y 102.3 no.** El enunciado pone "velocidades plausibles"
# MAGIC entre las compuertas, y 60 nudos es el límite físico adoptado en la Entrega 1; la pérdida medida
# MAGIC es mínima (83 filas el 2023-06-01). 102.3, en cambio, no es una velocidad imposible: es el
# MAGIC código AIS de "sin dato". Por eso solo anula el valor y deja la bandera `sog_no_disponible`.
# MAGIC
# MAGIC **Deduplicación con ganador determinístico.** Para cada llave `(MMSI, H3_Macro,
# MAGIC BaseDateTime)` gana la fila con menos campos "no disponible", luego la más completa
# MAGIC (nombre, IMO, indicativo, tipo, dimensiones), y por último el menor `record_hash` (sha256 del
# MAGIC contenido). Es un orden total sobre el *contenido*: el ganador es el mismo sin importar en
# MAGIC qué lote ni en qué orden lleguen las copias, así que la escritura es idempotente **y**
# MAGIC conmutativa. La misma regla se aplica dentro del lote (`row_number`) y contra la tabla
# MAGIC (`MERGE … WHEN MATCHED AND nueva < existente THEN UPDATE`).
# MAGIC
# MAGIC **Métricas como estado, no como log.** En cada micro-lote se recalculan **completas** las
# MAGIC fechas que el lote tocó (desde bronce, plata y cuarentena) y se escriben con `replaceWhere`
# MAGIC por fecha: se sobrescribe un estado completo, así que es seguro. El resultado es el mismo
# MAGIC si un día llega de una vez, con reenvíos o reprocesado desde cero. La fecha es la del
# MAGIC **nombre del archivo**, para poder contar también las filas sin timestamp legible.
# MAGIC
# MAGIC **Atomicidad entre tablas.** Plata, cuarentena y métricas son tres commits, no uno: Delta
# MAGIC no hace transacciones entre tablas. Lo compensa la idempotencia: si el lote falla a mitad,
# MAGIC el checkpoint no avanza, el lote se reintenta, los `MERGE` no reinsertan lo que ya estaba y
# MAGIC las métricas se recalculan. El estado final es el mismo.
# MAGIC
# MAGIC **`foreachBatch` en Spark Connect.** La función del lote se serializa y corre en un proceso
# MAGIC de Python del servidor, que no tiene acceso a `config.py`. Por eso dentro de ella solo se
# MAGIC usan valores simples (nombres de tablas como texto) y módulos de PySpark, y la sesión se
# MAGIC toma de `lote.sparkSession`.
# MAGIC
# MAGIC **Conciliación medida, no deducida.** Las métricas reclasifican bronce con las mismas reglas y
# MAGIC cuentan llaves válidas y contenidos inválidos distintos; los duplicados salen de ahí, no de restar.
# MAGIC `dif_plata` y `dif_cuarentena` comparan esos conteos contra las tablas: si el `MERGE` perdiera filas
# MAGIC dejarían de ser 0. Durante un backfill pueden diferir mientras el stream no alcanza a bronce;
# MAGIC el estado final, después del último lote, es el que se valida.

# COMMAND ----------

from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as f
from pyspark.sql.window import Window

import config

spark = config.obtener_spark()
w = config.obtener_workspace()

CATALOGO = config.leer_parametro("catalogo", config.CATALOGO_DEFECTO)
REPROCESAR = config.leer_parametro("reprocesar_desde_cero", "false").lower() == "true"
cfg = config.Config(CATALOGO)

# Constantes simples: son las únicas que puede usar la función de foreachBatch.
T_BRONZE = cfg.tabla("bronze", "ais_posiciones_raw")
T_SILVER = cfg.tabla("silver", "ais_posiciones")
T_CUARENTENA = cfg.tabla("silver", "ais_cuarentena")
T_METRICAS = cfg.tabla("silver", "calidad_metricas")
T_TIERRA = cfg.tabla("silver", "dim_h3_tierra_firme")
CHECKPOINT = cfg.ruta_checkpoint("silver")
RES_MICRO, RES_MACRO = config.MASCARA_RESOLUCION_H3, 3

# Contrato con la fuente: las columnas de bronce que plata conoce.
COLUMNAS_FUENTE = [
    "MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Heading", "VesselName", "IMO",
    "CallSign", "VesselType", "Status", "Length", "Width", "Draft", "Cargo", "TransceiverClass",
]
# Reglas duras en orden de prioridad (el orden define motivo_principal).
MOTIVOS = [
    "timestamp_invalido", "mmsi_invalido", "coordenada_nula", "coordenada_fuera_de_rango",
    "sog_implausible", "en_tierra",
]

# Los timestamps AIS están en UTC y no traen zona: se fija la zona de la sesión para que
# BaseDateTime y su fecha no dependan de la configuración del cómputo.
spark.conf.set("spark.sql.session.timeZone", "UTC")
print(f"{T_BRONZE}  ->  {T_SILVER} · {T_CUARENTENA} · {T_METRICAS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Contratos de las tablas
# MAGIC
# MAGIC `CLUSTER BY (fecha, MMSI)` en plata: el `MERGE` filtra por fecha y oro recalcula por
# MAGIC `(buque, fecha)`, así que agrupar físicamente por esas columnas reduce los archivos leídos.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_SILVER} (
    MMSI                     STRING    NOT NULL COMMENT 'Identificador del buque (9 dígitos)',
    BaseDateTime             TIMESTAMP NOT NULL COMMENT 'Momento del reporte (UTC)',
    LAT                      DOUBLE    NOT NULL COMMENT 'Latitud (grados)',
    LON                      DOUBLE    NOT NULL COMMENT 'Longitud (grados)',
    SOG                      DOUBLE             COMMENT 'Velocidad sobre el fondo (nudos); nula si no disponible o implausible',
    COG                      DOUBLE             COMMENT 'Rumbo sobre el fondo (grados); nulo si 360 (no disponible)',
    Heading                  DOUBLE             COMMENT 'Proa (grados); nulo si 511 (no disponible)',
    VesselName               STRING             COMMENT 'Nombre del buque',
    IMO                      STRING             COMMENT 'Número IMO',
    CallSign                 STRING             COMMENT 'Indicativo de llamada',
    VesselType               INT                COMMENT 'Código AIS de tipo de buque (ver dim_tipo_buque)',
    Status                   INT                COMMENT 'Estado de navegación AIS (0 navegando, 1 fondeado, 5 amarrado, ...)',
    Length                   DOUBLE             COMMENT 'Eslora (m)',
    Width                    DOUBLE             COMMENT 'Manga (m)',
    Draft                    DOUBLE             COMMENT 'Calado (m)',
    Cargo                    INT                COMMENT 'Código de carga AIS',
    TransceiverClass         STRING             COMMENT 'Clase del transpondedor (A/B)',
    fecha                    DATE      NOT NULL COMMENT 'Fecha UTC de BaseDateTime',
    H3_Micro                 BIGINT    NOT NULL COMMENT 'Celda H3 resolución 7 (~5 km²)',
    H3_Macro                 BIGINT    NOT NULL COMMENT 'Celda H3 resolución 3; parte de la llave (separa buques con el mismo MMSI en regiones distintas)',
    sog_no_disponible        BOOLEAN            COMMENT 'Regla suave: SOG = 102.3',
    
    cog_no_disponible        BOOLEAN            COMMENT 'Regla suave: COG = 360',
    heading_no_disponible    BOOLEAN            COMMENT 'Regla suave: Heading = 511',
    vessel_type_desconocido  BOOLEAN            COMMENT 'Regla suave: VesselType fuera de 0-99',
    n_no_disponibles         INT                COMMENT 'Desempate 1 (menor gana): campos cinemáticos no disponibles',
    n_completos              INT                COMMENT 'Desempate 2 (mayor gana): campos estáticos presentes',
    record_hash              STRING             COMMENT 'Desempate 3 (menor gana): sha256 del contenido original',
    _archivo                 STRING             COMMENT 'Archivo de origen de la fila ganadora',
    _fecha_archivo           DATE               COMMENT 'Fecha del archivo de origen (de su nombre)',
    _ingesta_ts              TIMESTAMP          COMMENT 'Ingesta a bronce de la fila ganadora',
    CONSTRAINT pk_ais_posiciones PRIMARY KEY (MMSI, H3_Macro, BaseDateTime)
)
CLUSTER BY (fecha, MMSI)
COMMENT 'Posiciones AIS que pasaron las compuertas de calidad: una fila por (MMSI, H3_Macro, BaseDateTime), elegida con un orden total sobre el contenido. Única fuente de oro.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_CUARENTENA} (
    {", ".join(f"{c} STRING" for c in COLUMNAS_FUENTE)},
    motivo_principal  STRING        NOT NULL COMMENT 'Primera regla dura que falla, en orden de prioridad',
    motivos           ARRAY<STRING> NOT NULL COMMENT 'Todas las reglas duras que falla la fila',
    record_hash       STRING        NOT NULL COMMENT 'sha256 del contenido original (llave de la cuarentena)',
    _archivo          STRING                 COMMENT 'Archivo de origen',
    _fecha_archivo    DATE                   COMMENT 'Fecha del archivo de origen (de su nombre)',
    _ingesta_ts       TIMESTAMP              COMMENT 'Ingesta a bronce',
    _rescued_data     STRING                 COMMENT 'Valores fuera del esquema, tal como venían de bronce'
)
CLUSTER BY (_fecha_archivo, motivo_principal)
COMMENT 'Registros AIS rechazados por las compuertas de plata, con los valores originales como texto y la causa documentada. Una fila por contenido distinto.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_METRICAS} (
    fecha          DATE      NOT NULL COMMENT 'Fecha del archivo de origen',
    metrica        STRING    NOT NULL COMMENT 'filas_recibidas, filas_silver, filas_cuarentena, duplicados_descartados, cuarentena_<motivo>, bandera_<regla>',
    valor          BIGINT    NOT NULL COMMENT 'Conteo',
    _calculado_ts  TIMESTAMP          COMMENT 'Cuándo se recalculó'
)
COMMENT 'Métricas de calidad por fecha, recalculadas completas en cada lote que toca la fecha (estado, no log).'
""")
print("Contratos declarados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transformación de un lote
# MAGIC
# MAGIC Funciones puras sobre DataFrames: reciben el lote de bronce y devuelven qué va a plata y
# MAGIC qué a cuarentena. Se separan de la escritura para poder probarlas sobre cualquier
# MAGIC DataFrame de bronce.

# COMMAND ----------

def fecha_de_archivo(columna_archivo):
    """AIS_2023_06_01.csv, AIS_2023_06_01_reenvio.csv, ... -> 2023-06-01."""
    return f.try_to_timestamp(
        f.regexp_extract(columna_archivo, r"^AIS_(\d{4}_\d{2}_\d{2})", 1), f.lit("yyyy_MM_dd")
    ).cast("date")


def clasificar(lote, tierra):
    """Tipa con try_cast, evalúa reglas duras y suaves. Devuelve el lote con columnas t_* y motivos."""
    tipado = (
        lote
        .withColumn("_fecha_archivo", fecha_de_archivo(f.col("_archivo")))
        .withColumn("record_hash", f.sha2(f.to_json(f.struct(*COLUMNAS_FUENTE)), 256))
        .withColumn("t_ts", f.try_to_timestamp("BaseDateTime"))
        .withColumn("t_lat", f.col("LAT").try_cast("double"))
        .withColumn("t_lon", f.col("LON").try_cast("double"))
        .withColumn("t_sog", f.col("SOG").try_cast("double"))
        .withColumn("t_cog", f.col("COG").try_cast("double"))
        .withColumn("t_heading", f.col("Heading").try_cast("double"))
        .withColumn("t_vessel_type", f.col("VesselType").try_cast("int"))
    )
    coord_ok = (
        f.col("t_lat").isNotNull() & f.col("t_lon").isNotNull()
        & f.col("t_lat").between(-90, 90) & f.col("t_lon").between(-180, 180)
    )
    tipado = (
        tipado
        .withColumn("H3_Micro", f.when(coord_ok, dbf.h3_longlatash3("t_lon", "t_lat", RES_MICRO)))
        .withColumn("H3_Macro", f.when(coord_ok, dbf.h3_longlatash3("t_lon", "t_lat", RES_MACRO)))
        .join(f.broadcast(tierra), on="H3_Micro", how="left")
    )
    condiciones = {
        "timestamp_invalido": f.col("t_ts").isNull(),
        "mmsi_invalido": f.col("MMSI").isNull() | ~f.col("MMSI").rlike(r"^[0-9]{9}$"),
        "coordenada_nula": f.col("t_lat").isNull() | f.col("t_lon").isNull(),
        "coordenada_fuera_de_rango": ~f.col("t_lat").between(-90, 90) | ~f.col("t_lon").between(-180, 180),
        "sog_implausible": ((f.col("t_sog") > 60) & (f.col("t_sog") != 102.3)) | (f.col("t_sog") < 0),
        "en_tierra": f.coalesce(f.col("_en_tierra"), f.lit(False)),
    }
    motivos = f.array_compact(
        f.array(*[f.when(f.coalesce(condiciones[m], f.lit(False)), f.lit(m)) for m in MOTIVOS])
    )
    return tipado.withColumn("motivos", motivos).withColumn("motivo_principal", f.try_element_at("motivos", f.lit(1)))


def a_silver(clasificado):
    """Filas válidas con el esquema de plata: reglas suaves aplicadas y columnas de desempate."""
    sog_nd = f.col("t_sog") == 102.3
    #sog_imp = (f.col("t_sog") > 60) & ~sog_nd | (f.col("t_sog") < 0)
    cog_nd = f.col("t_cog") == 360
    hdg_nd = f.col("t_heading") == 511
    no_disp = [sog_nd, cog_nd, hdg_nd]
    estaticos = ["VesselName", "IMO", "CallSign", "VesselType", "Length", "Width", "Draft"]
    return (
        clasificado.filter(f.size("motivos") == 0)
        .select(
            "MMSI",
            f.col("t_ts").alias("BaseDateTime"),
            f.col("t_lat").alias("LAT"),
            f.col("t_lon").alias("LON"),
            f.when(sog_nd, None).otherwise(f.col("t_sog")).alias("SOG"),
            f.when(cog_nd, None).otherwise(f.col("t_cog")).alias("COG"),
            f.when(hdg_nd, None).otherwise(f.col("t_heading")).alias("Heading"),
            "VesselName", "IMO", "CallSign",
            f.col("t_vessel_type").alias("VesselType"),
            f.col("Status").try_cast("int").alias("Status"),
            f.col("Length").try_cast("double").alias("Length"),
            f.col("Width").try_cast("double").alias("Width"),
            f.col("Draft").try_cast("double").alias("Draft"),
            f.col("Cargo").try_cast("int").alias("Cargo"),
            "TransceiverClass",
            f.to_date("t_ts").alias("fecha"),
            "H3_Micro", "H3_Macro",
            f.coalesce(sog_nd, f.lit(False)).alias("sog_no_disponible"),
            #f.coalesce(sog_imp, f.lit(False)).alias("sog_implausible"),
            f.coalesce(cog_nd, f.lit(False)).alias("cog_no_disponible"),
            f.coalesce(hdg_nd, f.lit(False)).alias("heading_no_disponible"),
            (f.col("VesselType").isNotNull()
             & (f.col("t_vessel_type").isNull() | ~f.col("t_vessel_type").between(0, 99))
             ).alias("vessel_type_desconocido"),
            sum(f.coalesce(c, f.lit(False)).cast("int") for c in no_disp).alias("n_no_disponibles"),
            sum((f.col(c).isNotNull() & (f.col(c) != "")).cast("int") for c in estaticos).alias("n_completos"),
            "record_hash", "_archivo", "_fecha_archivo", "_ingesta_ts",
        )
    )


def ganadores(df, llave, orden):
    """Una fila por llave según un orden total (row_number = 1)."""
    return (
        df.withColumn("_rn", f.row_number().over(Window.partitionBy(*llave).orderBy(*orden)))
        .filter(f.col("_rn") == 1)
        .drop("_rn")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Escritura de un lote (`foreachBatch`)
# MAGIC
# MAGIC 1. Clasificar el lote. 2. Plata: ganadores dentro del lote y `MERGE`. 3. Cuarentena:
# MAGIC una fila por `record_hash` y `MERGE` que solo inserta. 4. Métricas: recalcular completas las
# MAGIC fechas del lote y sobrescribirlas.

# COMMAND ----------

LLAVE = ["MMSI", "H3_Macro", "BaseDateTime"]
CONDICION_MEJOR = (
    "struct(s.n_no_disponibles, -s.n_completos, s.record_hash) "
    "< struct(t.n_no_disponibles, -t.n_completos, t.record_hash)"
)

def calcular_metricas(sesion, fechas, tierra):
    """Estado completo de calidad para las fechas dadas (formato largo: fecha, metrica, valor).

    La conciliación se mide por dos caminos independientes: bronce reclasificado con las mismas
    reglas (filas válidas, llaves válidas distintas, contenidos inválidos distintos) contra lo que
    de verdad quedó en plata y en cuarentena. Si el MERGE perdiera o duplicara filas, dif_plata ≠ 0;
    si la cuarentena perdiera filas, dif_cuarentena ≠ 0.
    """
    from datetime import timedelta  # import dentro: la función corre en el servidor

    en = lambda df, col: df.filter(f.col(col).isin(fechas))
    bronce = en(sesion.table(T_BRONZE).withColumn("fecha", fecha_de_archivo(f.col("_archivo"))), "fecha")
    clas = clasificar(bronce, tierra).withColumn("valida", f.size("motivos") == 0)
    llave = f.struct(f.col("MMSI"), f.col("H3_Macro"), f.col("t_ts"))  # la llave de plata, ya tipada
    medidas = clas.groupBy("fecha").agg(
        f.count("*").alias("filas_recibidas"),
        f.count("_rescued_data").alias("filas_esquema_rescatado"),
        f.count(f.when(f.col("valida"), 1)).alias("filas_validas_bronce"),
        f.count_distinct(f.when(f.col("valida"), llave)).alias("llaves_validas"),
        f.count(f.when(~f.col("valida"), 1)).alias("filas_invalidas_bronce"),
        f.count_distinct(f.when(~f.col("valida"), f.col("record_hash"))).alias("hashes_invalidos"),
    )

    # Poda por la llave de clustering de plata (`fecha`, ±1 día), igual que en la versión anterior.
    vecinas = sorted({d + timedelta(days=k) for d in fechas for k in (-1, 0, 1)})
    plata = (
        sesion.table(T_SILVER)
        .filter(f.col("fecha").isin(vecinas) & f.col("_fecha_archivo").isin(fechas))
        .withColumnRenamed("_fecha_archivo", "fecha_m")
    )
    cuar = en(sesion.table(T_CUARENTENA), "_fecha_archivo").withColumnRenamed("_fecha_archivo", "fecha_m")

    largo = lambda df, cols: df.select(
        "fecha", f.explode(f.map_from_arrays(f.array(*[f.lit(c) for c in cols]), f.array(*[f.col(c) for c in cols]))).alias("metrica", "valor")
    )
    banderas = ["sog_no_disponible", "cog_no_disponible", "heading_no_disponible", "vessel_type_desconocido"]
    en_plata = plata.groupBy(f.col("fecha_m").alias("fecha")).agg(
        f.count("*").alias("filas_silver"),
        *[f.count(f.when(f.col(b), 1)).alias(f"bandera_{b}") for b in banderas],
    )
    por_motivo = cuar.groupBy(f.col("fecha_m").alias("fecha")).pivot("motivo_principal", MOTIVOS).count()
    en_cuar = cuar.groupBy(f.col("fecha_m").alias("fecha")).agg(f.count("*").alias("filas_cuarentena"))

    ancho = (
        medidas.join(en_plata, "fecha", "left").join(en_cuar, "fecha", "left").join(por_motivo, "fecha", "left")
        .fillna(0)
    )
    for m in MOTIVOS:
        ancho = ancho.withColumnRenamed(m, f"cuarentena_{m}")
    ancho = (
        ancho
        .withColumn("duplicados_validos", f.col("filas_validas_bronce") - f.col("llaves_validas"))
        .withColumn("duplicados_cuarentena", f.col("filas_invalidas_bronce") - f.col("hashes_invalidos"))
        .withColumn("duplicados_descartados", f.col("duplicados_validos") + f.col("duplicados_cuarentena"))
        .withColumn("dif_plata", f.col("llaves_validas") - f.col("filas_silver"))
        .withColumn("dif_cuarentena", f.col("hashes_invalidos") - f.col("filas_cuarentena"))
    )
    metricas = [c for c in ancho.columns if c != "fecha"]
    return (
        largo(ancho.select("fecha", *[f.col(c).cast("bigint") for c in metricas]), metricas)
        .withColumn("_calculado_ts", f.current_timestamp())
    )

def procesar_lote(lote, batch_id):
    from delta.tables import DeltaTable  # import dentro: la función corre en el servidor

    sesion = lote.sparkSession
    tierra = sesion.table(T_TIERRA).select(f.col("h3_celda").alias("H3_Micro"), f.lit(True).alias("_en_tierra"))
    clasificado = clasificar(lote, tierra)

    # 1) Plata: ganador dentro del lote y MERGE contra la tabla
    orden_ganador = [f.col("n_no_disponibles").asc(), f.col("n_completos").desc(), f.col("record_hash").asc()]
    validos = ganadores(a_silver(clasificado), LLAVE, orden_ganador)
    fechas = [r["fecha"] for r in validos.select("fecha").distinct().collect()]
    if fechas:
        poda = "t.fecha IN (" + ", ".join(f"DATE'{d}'" for d in fechas) + ")"
        (
            DeltaTable.forName(sesion, T_SILVER).alias("t")
            .merge(validos.alias("s"), " AND ".join(f"t.{c} = s.{c}" for c in LLAVE) + f" AND {poda}")
            .whenMatchedUpdateAll(condition=CONDICION_MEJOR)
            .whenNotMatchedInsertAll()
            .execute()
        )

    # 2) Cuarentena: una fila por contenido; el MERGE solo inserta lo que no estaba
    rechazados = ganadores(
        clasificado.filter(f.size("motivos") > 0).select(
            *COLUMNAS_FUENTE, "motivo_principal", "motivos", "record_hash",
            "_archivo", "_fecha_archivo", "_ingesta_ts", "_rescued_data",
        ),
        ["record_hash"], [f.col("_archivo").asc()],
    )
    (
        DeltaTable.forName(sesion, T_CUARENTENA).alias("t")
        .merge(rechazados.alias("s"), "t.record_hash = s.record_hash")
        .whenNotMatchedInsertAll()
        .execute()
    )

    # 3) Métricas: estado completo de las fechas de archivo que tocó el lote
    fechas_archivo = [
        r["_fecha_archivo"] for r in lote.select(fecha_de_archivo(f.col("_archivo")).alias("_fecha_archivo")).distinct().collect()
        if r["_fecha_archivo"] is not None
    ]
    if fechas_archivo:
        lista = ", ".join(f"DATE'{d}'" for d in fechas_archivo)
        (
            calcular_metricas(sesion, fechas_archivo, tierra).write.format("delta").mode("overwrite")
            .option("replaceWhere", f"fecha IN ({lista})")
            .saveAsTable(T_METRICAS)
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ejecución del stream

# COMMAND ----------

def borrar_carpeta(ruta: str) -> None:
    try:
        entradas = list(w.files.list_directory_contents(ruta))
    except Exception:
        return
    for e in entradas:
        borrar_carpeta(e.path) if e.is_directory else w.files.delete(e.path)
    w.files.delete_directory(ruta)


if REPROCESAR:
    print(f"reprocesar_desde_cero=true: se borra el checkpoint {CHECKPOINT}")
    borrar_carpeta(CHECKPOINT)


def correr_silver(nombre: str) -> list:
    consulta = (
        spark.readStream.table(T_BRONZE)
        .writeStream.queryName(nombre)
        .foreachBatch(procesar_lote)
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )
    consulta.awaitTermination()
    return [p for p in consulta.recentProgress if (p.numInputRows or 0) > 0]


lotes = correr_silver("silver_ingesta")
print(f"Micro-lotes procesados: {len(lotes)}")
for p in lotes:
    print(f"  lote {p.batchId}: {p.numInputRows:>12,} filas de bronce "
          f"({(p.durationMs or {}).get('triggerExecution', 0) / 1000:,.1f} s)")

# COMMAND ----------

# MAGIC %md
# MAGIC # Evidencia
# MAGIC
# MAGIC
# MAGIC ## Idempotencia del checkpoint
# MAGIC
# MAGIC Igual que en bronce: lanzar el stream otra vez de inmediato no debe escribir nada. Se compara la
# MAGIC versión Delta de plata, cuarentena y métricas antes y después, en lugar de `recentProgress`, que en
# MAGIC serverless puede llegar vacío.

# COMMAND ----------

def version_actual(tabla: str) -> int:
    """Última versión de la tabla Delta."""
    return spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 1").first().version


tablas_plata = [T_SILVER, T_CUARENTENA, T_METRICAS]
antes = {t: version_actual(t) for t in tablas_plata}
correr_silver("silver_verificacion")
despues = {t: version_actual(t) for t in tablas_plata}
for t in tablas_plata:
    print(f"{t}: versión {antes[t]} → {despues[t]}")
assert antes == despues, "La segunda corrida de plata escribió: el checkpoint volvió a procesar versiones de bronce"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unicidad por buque y timestamp
# MAGIC
# MAGIC `COUNT(*)` debe ser igual a `COUNT(DISTINCT llave)` en plata.

# COMMAND ----------

u = spark.table(T_SILVER).agg(
    f.count("*").alias("filas"), f.count_distinct(*LLAVE).alias("llaves")
).first()
print(f"Plata: {u.filas:,} filas, {u.llaves:,} llaves distintas")
assert u.filas == u.llaves, "Hay llaves duplicadas en plata"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filas inyectadas: cada una donde debía
# MAGIC
# MAGIC El productor inyecta una fila por motivo (con `VesselName = INYECTADO_<caso>`) y un control
# MAGIC negativo con `SOG = 102.3`, que debe quedar en plata con `SOG` nulo y su bandera.

# COMMAND ----------

esperado = {
    "INYECTADO_lat_fuera_de_rango": "coordenada_fuera_de_rango",
    "INYECTADO_lon_fuera_de_rango": "coordenada_fuera_de_rango",
    "INYECTADO_coordenada_nula": "coordenada_nula",
    "INYECTADO_en_tierra": "en_tierra",
    "INYECTADO_mmsi_mal_formado": "mmsi_invalido",
    "INYECTADO_mmsi_nulo": "mmsi_invalido",
    "INYECTADO_timestamp_ilegible": "timestamp_invalido",
    "INYECTADO_timestamp_nulo": "timestamp_invalido",
    "INYECTADO_sog_implausible": "sog_implausible",
}
filas_iny = (
    spark.table(T_CUARENTENA).filter(f.col("VesselName").startswith("INYECTADO_"))
    .select("VesselName", "motivo_principal").collect()
)
# Si un caso aparece en varias fechas con motivos distintos, `obtenido` lo marca como inconsistente.
obtenido = {}
for r in filas_iny:
    obtenido[r.VesselName] = r.motivo_principal if obtenido.get(r.VesselName, r.motivo_principal) == r.motivo_principal else "INCONSISTENTE"
for caso, motivo in esperado.items():
    marca = "OK " if obtenido.get(caso) == motivo else "FALLA"
    print(f"[{marca}] {caso:32s} esperado={motivo:27s} obtenido={obtenido.get(caso)}")

# Puede haber inyectadas de varias fechas: cada fila debe tener su motivo y cada control
# (uno por fecha inyectada) debe quedar en plata con SOG nulo y su bandera.
fechas_iny = [r._fecha_archivo for r in spark.table(T_CUARENTENA)
              .filter(f.col("VesselName").startswith("INYECTADO_")).select("_fecha_archivo").distinct().collect()]
control = (
    spark.table(T_SILVER)
    .filter(f.col("fecha").isin(fechas_iny) & (f.col("VesselName") == "CONTROL_sog_no_disponible"))
    .collect()
) if fechas_iny else []
ok_control = len(control) == len(fechas_iny) and all(c.SOG is None and c.sog_no_disponible for c in control)
print(f"[{'OK ' if ok_control else 'FALLA'}] CONTROL_sog_no_disponible en plata con SOG nulo y bandera "
      f"({len(control)} de {len(fechas_iny)} fecha(s) inyectada(s))")

if fechas_iny:  # solo si se entregó algún archivo de inyectados
    assert obtenido == esperado and ok_control, "Alguna fila inyectada no terminó donde debía"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Métricas de calidad y balance
# MAGIC
# MAGIC Para cada fecha: `filas_recibidas = filas_silver + filas_cuarentena + duplicados_descartados`.

# COMMAND ----------

(
    spark.table(T_METRICAS)
    .groupBy("fecha").pivot("metrica").agg(f.first("valor"))
    .orderBy("fecha")
    .show(truncate=False, vertical=True)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historial de `MERGE` en plata
# MAGIC
# MAGIC Cada lote deja un commit con cuántas filas insertó, actualizó o dejó igual. Un reproceso
# MAGIC (`reprocesar_desde_cero = true`) debe mostrar 0 insertadas y 0 actualizadas.

# COMMAND ----------

(
    spark.sql(f"DESCRIBE HISTORY {T_SILVER}")
    .filter(f.col("operation") == "MERGE")
    .select(
        "version", "timestamp",
        f.col("operationMetrics.numSourceRows").alias("filas_lote"),
        f.col("operationMetrics.numTargetRowsInserted").alias("insertadas"),
        f.col("operationMetrics.numTargetRowsUpdated").alias("actualizadas"),
    )
    .orderBy("version")
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conciliación medida
# MAGIC
# MAGIC Dos caminos independientes deben coincidir. Por fecha: `dif_plata` (llaves válidas distintas en
# MAGIC bronce − filas en plata) y `dif_cuarentena` (contenidos inválidos distintos en bronce − filas en
# MAGIC cuarentena) deben ser 0. Global, sin depender de la fecha del archivo: las llaves válidas
# MAGIC distintas de todo bronce deben ser igual a las filas de plata. Antes, los duplicados se obtenían
# MAGIC restando y el balance cuadraba por construcción; ahora una fila perdida en el `MERGE` lo rompe.

# COMMAND ----------

conc = (
    spark.table(T_METRICAS)
    .groupBy("fecha").pivot("metrica", [
        "filas_recibidas", "filas_silver", "filas_cuarentena",
        "duplicados_validos", "duplicados_cuarentena", "dif_plata", "dif_cuarentena",
    ]).agg(f.first("valor"))
    .orderBy("fecha")
)
conc.show(truncate=False)

# Global: reclasifica todo bronce con las mismas reglas y compara contra las tablas.
tierra = spark.table(T_TIERRA).select(f.col("h3_celda").alias("H3_Micro"), f.lit(True).alias("_en_tierra"))
clas_total = clasificar(spark.table(T_BRONZE), tierra)
llaves_bronce = clas_total.filter(f.size("motivos") == 0).select("MMSI", "H3_Macro", "t_ts").distinct().count()
hashes_bronce = clas_total.filter(f.size("motivos") > 0).select("record_hash").distinct().count()
filas_plata, filas_cuar = spark.table(T_SILVER).count(), spark.table(T_CUARENTENA).count()
print(f"Global · llaves válidas distintas en bronce {llaves_bronce:,} · filas en plata {filas_plata:,}")
print(f"Global · contenidos inválidos distintos en bronce {hashes_bronce:,} · filas en cuarentena {filas_cuar:,}")

fechas_con_dif = conc.filter((f.col("dif_plata") != 0) | (f.col("dif_cuarentena") != 0)).count()
assert llaves_bronce == filas_plata and hashes_bronce == filas_cuar, "La conciliación global no cuadra"
assert fechas_con_dif == 0, "Hay fechas con dif_plata o dif_cuarentena distinta de 0"
print("Concilia: cada fila de bronce quedó en plata, en cuarentena o contada como duplicado medido.")
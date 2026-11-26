# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 · Oro: trayectorias, tráfico por celda y hora, presencia en puertos
# MAGIC
# MAGIC **OceanWatch Analytics · Entrega 2** · Tarea 5 del job · Mapa completo del pipeline en el
# MAGIC [README](../README.md#entrega-2--pipeline-en-streaming).
# MAGIC
# MAGIC ## Propósito
# MAGIC
# MAGIC Construir el modelo del negocio a partir de plata: las tres familias de tablas que pide el
# MAGIC enunciado (*"trayectorias diarias por buque, presencia en puertos, matrices de tráfico por
# MAGIC celda espacial y hora"*) y que alimentan el tablero y el modelo de ML. Oro **solo lee
# MAGIC `silver.ais_posiciones`**: un registro que no pasó las compuertas no llega aquí.
# MAGIC
# MAGIC ## Entradas y salidas
# MAGIC
# MAGIC | | Objeto | Detalle |
# MAGIC |---|---|---|
# MAGIC | **Entrada** | `silver.ais_posiciones` | Posiciones válidas y únicas |
# MAGIC | **Entrada** | `silver.calidad_metricas` | Qué fechas cambiaron en plata y cuándo (`_calculado_ts`) |
# MAGIC | **Entrada** | `silver.dim_tipo_buque`, `silver.dim_puertos` | Categoría de buque, puertos |
# MAGIC | **Salida** | `gold.trayectorias_puntos` | Cada posición con su punto anterior, distancia, velocidad implícita y viaje |
# MAGIC | **Salida** | `gold.trayectorias_diarias` | Un viaje por fila: `(MMSI_Real, fecha, Trip_ID)` |
# MAGIC | **Salida** | `gold.trafico_celda_hora` | Buques, posiciones y SOG por celda H3 res 5, hora y categoría |
# MAGIC | **Salida** | `gold.presencia_puertos` | Un buque en un puerto en un día, con arribos y zarpes |
# MAGIC | **Salida** | `gold.visitas_puerto` (vista) | Visitas completas reconstruidas uniendo días |
# MAGIC
# MAGIC **Parámetros:** `catalogo`; `recalcular_todo` (`false` por defecto; `true` recalcula todas
# MAGIC las fechas de plata).
# MAGIC
# MAGIC ## Decisiones
# MAGIC
# MAGIC - **Batch incremental, no stream.** Plata recibe `MERGE` (actualiza filas cuando llega una
# MAGIC   copia mejor de una llave), y un stream sobre una tabla con actualizaciones falla. Además,
# MAGIC   oro recalcula días completos (el `lag` de las trayectorias necesita todos los puntos del
# MAGIC   día): es trabajo batch. Igual que en la semana 10 y el parcial, oro es batch sobre la capa
# MAGIC   limpia; aquí, además, incremental.
# MAGIC - **Qué recalcular: se le pregunta al destino.** `silver.calidad_metricas` renueva
# MAGIC   `_calculado_ts` de una fecha cada vez que un lote de plata la toca. Cada tabla de oro guarda
# MAGIC   en `_version_silver` el `_calculado_ts` con que se construyó cada fecha. Se recalculan las
# MAGIC   fechas donde plata es más nueva, **más el día siguiente** (sus trayectorias y presencias
# MAGIC   dependen del último punto del día anterior). Es el mismo patrón de `_version_fuente` de las
# MAGIC   dimensiones. Supuesto: la fecha del archivo coincide con la de `BaseDateTime` (medido: 0
# MAGIC   filas fuera del día en 7 días); recalcular D+1 cubre el caso no observado.
# MAGIC - **Escritura atómica por tabla:** `replaceWhere` sobre las fechas recalculadas; se
# MAGIC   sobrescribe el estado completo de esas fechas, así que es seguro e idempotente.
# MAGIC - **Trayectorias por `(MMSI_Real, fecha)`**, con la lógica de la Entrega 1: `MMSI_Real =
# MAGIC   MMSI + H3_Macro` (separa buques distintos con el mismo MMSI), corte de viaje si el salto
# MAGIC   supera 2 celdas H3 res 7, si hay más de 12 h sin reportar o si la velocidad implícita
# MAGIC   supera 60 nudos; la distancia de un tramo cortado vale 0 (candado cinemático). El primer
# MAGIC   punto de cada día **mira el último del día anterior**: el tramo que cruza la medianoche
# MAGIC   tiene distancia, y el primer viaje del día queda con `continua_de` si no hubo corte.
# MAGIC - **Presencia en puertos con grano diario** `(fecha, puerto, buque)`, no por visita: una
# MAGIC   visita puede durar días y recalcularla obligaría a una cascada sin límite. Cada día
# MAGIC   depende solo de sí mismo y del anterior. Las visitas completas se reconstruyen en una vista
# MAGIC   (*gaps and islands*), que nunca queda desactualizada.
# MAGIC - **Lectura de una tabla recién escrita como "caché".** Serverless no permite `cache()`:
# MAGIC   `trayectorias_puntos` se escribe primero y las demás tablas la leen de Delta en vez de
# MAGIC   recalcular las ventanas.

# COMMAND ----------

from datetime import timedelta

from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as f
from pyspark.sql.window import Window

import config

spark = config.obtener_spark()
spark.conf.set("spark.sql.session.timeZone", "UTC")

CATALOGO = config.leer_parametro("catalogo", config.CATALOGO_DEFECTO)
RECALCULAR_TODO = config.leer_parametro("recalcular_todo", "false").lower() == "true"
cfg = config.Config(CATALOGO)

T_SILVER = cfg.tabla("silver", "ais_posiciones")
T_METRICAS = cfg.tabla("silver", "calidad_metricas")
T_TIPOS = cfg.tabla("silver", "dim_tipo_buque")
T_PUERTOS = cfg.tabla("silver", "dim_puertos")
T_PUNTOS = cfg.tabla("gold", "trayectorias_puntos")
T_DIARIAS = cfg.tabla("gold", "trayectorias_diarias")
T_TRAFICO = cfg.tabla("gold", "trafico_celda_hora")
T_PRESENCIA = cfg.tabla("gold", "presencia_puertos")
V_VISITAS = cfg.tabla("gold", "visitas_puerto")
TABLAS_ORO = [T_PUNTOS, T_DIARIAS, T_TRAFICO, T_PRESENCIA]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Contratos de las tablas de oro
# MAGIC
# MAGIC Es el contrato de datos de la capa oro (requisito 5): esquema explícito y comentario en
# MAGIC cada columna. Todas llevan `fecha` (para el `replaceWhere`) y `_version_silver`.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_PUNTOS} (
    MMSI_Real               STRING    NOT NULL COMMENT 'MMSI + H3_Macro: separa buques distintos que transmiten el mismo MMSI',
    MMSI                    STRING    NOT NULL COMMENT 'Identificador del buque',
    fecha                   DATE      NOT NULL COMMENT 'Fecha UTC de la posición',
    BaseDateTime            TIMESTAMP NOT NULL COMMENT 'Momento del reporte (UTC)',
    LAT                     DOUBLE    NOT NULL COMMENT 'Latitud',
    LON                     DOUBLE    NOT NULL COMMENT 'Longitud',
    SOG                     DOUBLE             COMMENT 'Velocidad reportada (nudos), nula si no disponible',
    Status                  INT                COMMENT 'Estado de navegación AIS',
    VesselType              INT                COMMENT 'Código AIS de tipo de buque',
    H3_Micro                BIGINT    NOT NULL COMMENT 'Celda H3 res 7',
    horas_desde_prev        DOUBLE             COMMENT 'Horas desde el reporte anterior del mismo MMSI_Real (puede ser del día anterior)',
    distancia_nm            DOUBLE             COMMENT 'Distancia geodésica al punto anterior (millas náuticas)',
    velocidad_implicita_kn  DOUBLE             COMMENT 'distancia_nm / horas_desde_prev: valida el movimiento independientemente del SOG',
    salto_h3                INT                COMMENT 'Celdas H3 res 7 entre el punto anterior y este',
    anomalia_velocidad      BOOLEAN   NOT NULL COMMENT 'Velocidad implícita > 60 nudos (salto de GPS o MMSI compartido)',
    corte                   BOOLEAN   NOT NULL COMMENT 'Empieza un viaje nuevo: sin punto anterior, salto > 2 celdas, silencio > 12 h o velocidad imposible',
    distancia_segmento_nm   DOUBLE    NOT NULL COMMENT 'Distancia sumable del tramo: 0 si hubo corte (candado cinemático)',
    Trip_ID                 INT       NOT NULL COMMENT 'Viaje dentro de (MMSI_Real, fecha), desde 1',
    continua_de_fecha       DATE               COMMENT 'Solo en el primer punto del día: fecha del viaje del que es continuación',
    continua_de_trip        INT                COMMENT 'Solo en el primer punto del día: Trip_ID del viaje del que es continuación',
    _version_silver         TIMESTAMP NOT NULL COMMENT '_calculado_ts de plata con que se construyó esta fecha'
)
CLUSTER BY (fecha, MMSI)
COMMENT 'Posiciones de plata enriquecidas con el punto anterior del mismo buque, distancia, velocidad implícita y segmentación en viajes diarios.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_DIARIAS} (
    MMSI_Real                   STRING    NOT NULL COMMENT 'MMSI + H3_Macro',
    MMSI                        STRING    NOT NULL COMMENT 'Identificador del buque',
    fecha                       DATE      NOT NULL COMMENT 'Fecha UTC del viaje (los viajes se cortan a medianoche)',
    Trip_ID                     INT       NOT NULL COMMENT 'Viaje dentro de (MMSI_Real, fecha)',
    VesselType                  INT                COMMENT 'Código AIS de tipo de buque',
    inicio                      TIMESTAMP NOT NULL COMMENT 'Primer reporte del viaje',
    fin                         TIMESTAMP NOT NULL COMMENT 'Último reporte del viaje',
    duracion_h                  DOUBLE    NOT NULL COMMENT 'fin - inicio, en horas',
    puntos                      BIGINT    NOT NULL COMMENT 'Reportes del viaje',
    distancia_nm                DOUBLE    NOT NULL COMMENT 'Suma de distancia_segmento_nm',
    sog_promedio                DOUBLE             COMMENT 'Promedio del SOG reportado',
    velocidad_implicita_max_kn  DOUBLE             COMMENT 'Máxima velocidad implícita entre puntos del viaje',
    anomalias_velocidad         BIGINT    NOT NULL COMMENT 'Puntos con velocidad implícita > 60 nudos',
    lat_inicio                  DOUBLE    NOT NULL COMMENT 'Latitud del primer reporte',
    lon_inicio                  DOUBLE    NOT NULL COMMENT 'Longitud del primer reporte',
    lat_fin                     DOUBLE    NOT NULL COMMENT 'Latitud del último reporte',
    lon_fin                     DOUBLE    NOT NULL COMMENT 'Longitud del último reporte',
    continua_de_fecha           DATE               COMMENT 'Si el viaje sigue de la víspera: fecha del viaje anterior',
    continua_de_trip            INT                COMMENT 'Si el viaje sigue de la víspera: Trip_ID del viaje anterior',
    _version_silver             TIMESTAMP NOT NULL COMMENT '_calculado_ts de plata con que se construyó esta fecha'
)
CLUSTER BY (fecha, MMSI)
COMMENT 'Trayectorias diarias por buque: un viaje por fila. Un viaje de varios días se sigue por la cadena continua_de.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_TRAFICO} (
    fecha            DATE      NOT NULL COMMENT 'Fecha UTC',
    hora             INT       NOT NULL COMMENT 'Hora UTC (0-23)',
    h3_celda         BIGINT    NOT NULL COMMENT 'Celda H3 resolución {config.RES_TRAFICO} (~250 km²)',
    categoria        STRING    NOT NULL COMMENT 'Categoría de buque (dim_tipo_buque) o Desconocido',
    buques           BIGINT    NOT NULL COMMENT 'Buques distintos (MMSI) en la celda y hora',
    posiciones       BIGINT    NOT NULL COMMENT 'Reportes en la celda y hora',
    sog_promedio     DOUBLE             COMMENT 'Promedio del SOG reportado',
    _version_silver  TIMESTAMP NOT NULL COMMENT '_calculado_ts de plata con que se construyó esta fecha'
)
CLUSTER BY (fecha, h3_celda)
COMMENT 'Matriz de tráfico por celda espacial (H3 res {config.RES_TRAFICO}), hora y categoría de buque. Base del mapa de intensidad.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_PRESENCIA} (
    fecha               DATE      NOT NULL COMMENT 'Fecha UTC',
    wpi_id              INT       NOT NULL COMMENT 'Puerto (World Port Index Number)',
    nombre_puerto       STRING             COMMENT 'Nombre del puerto',
    pais                STRING             COMMENT 'País del puerto',
    MMSI_Real           STRING    NOT NULL COMMENT 'MMSI + H3_Macro',
    MMSI                STRING    NOT NULL COMMENT 'Identificador del buque',
    VesselType          INT                COMMENT 'Código AIS de tipo de buque',
    primera_ts          TIMESTAMP          COMMENT 'Primer reporte en puerto en el día (nulo si solo hubo zarpe)',
    ultima_ts           TIMESTAMP          COMMENT 'Último reporte en puerto en el día',
    posiciones          BIGINT    NOT NULL COMMENT 'Reportes en puerto en el día',
    horas_en_puerto     DOUBLE    NOT NULL COMMENT 'Suma de intervalos ≤ 1 h entre reportes consecutivos en el mismo puerto',
    arribos             BIGINT    NOT NULL COMMENT 'Entradas al puerto en el día (el reporte anterior estaba fuera de este puerto)',
    zarpes              BIGINT    NOT NULL COMMENT 'Salidas del puerto en el día (el reporte anterior estaba en este puerto)',
    presente_al_inicio  BOOLEAN   NOT NULL COMMENT 'Primer reporte conocido del buque ya en puerto: estaba ahí antes de los datos (no es arribo)',
    _version_silver     TIMESTAMP NOT NULL COMMENT '_calculado_ts de plata con que se construyó esta fecha'
)
CLUSTER BY (fecha, wpi_id)
COMMENT 'Presencia diaria de buques en puertos. En puerto = dentro de {config.PUERTO_K_ANILLOS} anillos H3 res {config.PUERTO_RES_H3} del punto del WPI (puerto más cercano) y (Status fondeado/amarrado o SOG < {config.EN_PUERTO_SOG_MAX} nudo). Base de buques en puerto, ranking y arribos para ML.'
""")
print("Contratos declarados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Qué fechas recalcular

# COMMAND ----------

version_silver = (
    spark.table(T_METRICAS).groupBy("fecha").agg(f.max("_calculado_ts").alias("v_silver"))
)
estado = version_silver
for i, t in enumerate(TABLAS_ORO):
    estado = estado.join(
        spark.table(t).groupBy("fecha").agg(f.max("_version_silver").alias(f"v{i}")), "fecha", "left"
    )
desactualizada = f.lit(RECALCULAR_TODO)
for i in range(len(TABLAS_ORO)):
    desactualizada = desactualizada | f.col(f"v{i}").isNull() | (f.col(f"v{i}") < f.col("v_silver"))

filas_estado = estado.select("fecha", "v_silver", desactualizada.alias("pendiente")).collect()
VERSIONES = {r.fecha: r.v_silver for r in filas_estado}
pendientes = {r.fecha for r in filas_estado if r.pendiente}
# El día siguiente depende del último punto de este: también se recalcula si existe en plata.
FECHAS = sorted(pendientes | {d + timedelta(days=1) for d in pendientes if d + timedelta(days=1) in VERSIONES})
print(f"Fechas en plata: {len(VERSIONES)} · por recalcular: {[str(d) for d in FECHAS] or 'ninguna'}")

# COMMAND ----------

def escribir_fechas(df, tabla: str) -> None:
    """Sobrescribe solo las fechas recalculadas (estado completo de esas fechas → atómico e idempotente)."""
    lista = ", ".join(f"DATE'{d}'" for d in FECHAS)
    df.write.format("delta").mode("overwrite").option("replaceWhere", f"fecha IN ({lista})").saveAsTable(tabla)
    print(f"  {tabla}: {spark.table(tabla).filter(f.col('fecha').isin(FECHAS)).count():,} filas en {len(FECHAS)} fecha(s)")


versiones_df = (
    spark.createDataFrame([(d, VERSIONES[d]) for d in FECHAS], "fecha DATE, _version_silver TIMESTAMP")
    if FECHAS else None
)
# Fechas cargadas: las que se recalculan y la víspera de cada una (para el punto anterior).
FECHAS_CARGA = sorted(set(FECHAS) | {d - timedelta(days=1) for d in FECHAS})
w_buque = Window.partitionBy("MMSI_Real").orderBy("BaseDateTime")
w_dia = Window.partitionBy("MMSI_Real", "fecha").orderBy("BaseDateTime")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `trayectorias_puntos`
# MAGIC
# MAGIC Se cargan las fechas a recalcular **y su víspera**; el `lag` sobre `MMSI_Real` (sin partir
# MAGIC por fecha) trae el punto anterior aunque sea del día previo. Después se numeran los viajes
# MAGIC dentro de cada `(MMSI_Real, fecha)` y solo se escriben las fechas a recalcular.

# COMMAND ----------

if FECHAS:
    base = (
        spark.table(T_SILVER)
        .filter(f.col("fecha").isin(FECHAS_CARGA))
        .withColumn("MMSI_Real", f.concat_ws("_", "MMSI", f.col("H3_Macro").cast("string")))
    )
    con_prev = (
        base
        .withColumn("prev_ts", f.lag("BaseDateTime").over(w_buque))
        .withColumn("prev_lat", f.lag("LAT").over(w_buque))
        .withColumn("prev_lon", f.lag("LON").over(w_buque))
        .withColumn("prev_h3", f.lag("H3_Micro").over(w_buque))
        .withColumn("horas_desde_prev",
                    (f.col("BaseDateTime").cast("long") - f.col("prev_ts").cast("long")) / 3600.0)
        .withColumn("distancia_nm", f.when(
            f.col("prev_ts").isNotNull(),
            dbf.st_distancesphere(dbf.st_point("prev_lon", "prev_lat"), dbf.st_point("LON", "LAT")) / 1852.0))
        .withColumn("velocidad_implicita_kn", f.when(
            f.col("horas_desde_prev") > 0, f.col("distancia_nm") / f.col("horas_desde_prev")))
        .withColumn("salto_h3", dbf.h3_try_distance("prev_h3", "H3_Micro"))
        .withColumn("anomalia_velocidad",
                    f.coalesce(f.col("velocidad_implicita_kn") > config.VELOCIDAD_MAX_KN, f.lit(False)))
        .withColumn("corte", (
            f.col("prev_ts").isNull()
            | f.coalesce(f.col("salto_h3") > config.VIAJE_MAX_SALTO_H3, f.lit(False))
            | f.coalesce(f.col("horas_desde_prev") > config.VIAJE_MAX_SILENCIO_H, f.lit(False))
            | f.col("anomalia_velocidad")
        ))
    )
    primero_del_dia = f.row_number().over(w_dia) == 1
    con_viaje = (
        con_prev
        .withColumn("primero_del_dia", primero_del_dia)
        .withColumn("Trip_ID", f.sum((f.col("primero_del_dia") | f.col("corte")).cast("int")).over(
            w_dia.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        .withColumn("distancia_segmento_nm",
                    f.when(f.col("corte"), 0.0).otherwise(f.coalesce("distancia_nm", f.lit(0.0))))
        .withColumn("prev_trip", f.lag("Trip_ID").over(w_buque))
        .withColumn("prev_fecha", f.lag("fecha").over(w_buque))
        .withColumn("continua", f.col("primero_del_dia") & ~f.col("corte"))
        .withColumn("continua_de_fecha", f.when(f.col("continua"), f.col("prev_fecha")))
        .withColumn("continua_de_trip", f.when(f.col("continua"), f.col("prev_trip")))
    )
    puntos = (
        con_viaje.filter(f.col("fecha").isin(FECHAS))
        .join(versiones_df, "fecha")
        .select(
            "MMSI_Real", "MMSI", "fecha", "BaseDateTime", "LAT", "LON", "SOG", "Status", "VesselType",
            "H3_Micro", "horas_desde_prev", "distancia_nm", "velocidad_implicita_kn",
            f.col("salto_h3").cast("int").alias("salto_h3"), "anomalia_velocidad", "corte",
            "distancia_segmento_nm", f.col("Trip_ID").cast("int").alias("Trip_ID"),
            "continua_de_fecha", f.col("continua_de_trip").cast("int").alias("continua_de_trip"),
            "_version_silver",
        )
    )
    escribir_fechas(puntos, T_PUNTOS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `trayectorias_diarias`

# COMMAND ----------

if FECHAS:
    diarias = (
        spark.table(T_PUNTOS).filter(f.col("fecha").isin(FECHAS))
        .groupBy("MMSI_Real", "MMSI", "fecha", "Trip_ID")
        .agg(
            f.max_by("VesselType", "BaseDateTime").alias("VesselType"),
            f.min("BaseDateTime").alias("inicio"),
            f.max("BaseDateTime").alias("fin"),
            f.count("*").alias("puntos"),
            f.sum("distancia_segmento_nm").alias("distancia_nm"),
            f.avg("SOG").alias("sog_promedio"),
            f.max("velocidad_implicita_kn").alias("velocidad_implicita_max_kn"),
            f.count(f.when(f.col("anomalia_velocidad"), 1)).alias("anomalias_velocidad"),
            f.min_by("LAT", "BaseDateTime").alias("lat_inicio"),
            f.min_by("LON", "BaseDateTime").alias("lon_inicio"),
            f.max_by("LAT", "BaseDateTime").alias("lat_fin"),
            f.max_by("LON", "BaseDateTime").alias("lon_fin"),
            f.max("continua_de_fecha").alias("continua_de_fecha"),
            f.max("continua_de_trip").alias("continua_de_trip"),
            f.max("_version_silver").alias("_version_silver"),
        )
        .withColumn("duracion_h", (f.col("fin").cast("long") - f.col("inicio").cast("long")) / 3600.0)
    )
    escribir_fechas(diarias, T_DIARIAS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `trafico_celda_hora`

# COMMAND ----------

if FECHAS:
    categorias = spark.table(T_TIPOS).select(f.col("vessel_type").alias("VesselType"), "categoria")
    trafico = (
        spark.table(T_PUNTOS).filter(f.col("fecha").isin(FECHAS))
        .join(f.broadcast(categorias), "VesselType", "left")
        .groupBy(
            "fecha",
            f.hour("BaseDateTime").alias("hora"),
            dbf.h3_toparent("H3_Micro", config.RES_TRAFICO).alias("h3_celda"),
            f.coalesce("categoria", f.lit("Desconocido")).alias("categoria"),
        )
        .agg(
            f.count_distinct("MMSI").alias("buques"),
            f.count("*").alias("posiciones"),
            f.avg("SOG").alias("sog_promedio"),
            f.max("_version_silver").alias("_version_silver"),
        )
    )
    escribir_fechas(trafico, T_TRAFICO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `presencia_puertos`
# MAGIC
# MAGIC 1. **Área de cada puerto** (Entrega 1): el punto del WPI en H3 res 8 y un anillo de 5
# MAGIC    celdas; si dos puertos comparten una celda, queda el más cercano.
# MAGIC 2. **En puerto:** en el área y (`Status` fondeado/amarrado o `SOG` < 1 nudo).
# MAGIC 3. **Eventos** comparando con el reporte anterior del mismo buque (que puede ser de la
# MAGIC    víspera): `arribo` si antes estaba fuera de este puerto; `zarpe` si antes estaba en un
# MAGIC    puerto y ahora no (o en otro). Si el buque no tiene reporte anterior y ya está en puerto,
# MAGIC    es `presente_al_inicio`: estaba ahí antes de los datos, no es un arribo (importante para
# MAGIC    no inflar los arribos del primer día en el modelo de ML).
# MAGIC 4. **Horas en puerto:** suma de los intervalos entre reportes consecutivos en el mismo puerto
# MAGIC    de hasta 1 h (un silencio más largo no se cuenta como tiempo observado en puerto).

# COMMAND ----------

if FECHAS:
    w_celda = Window.partitionBy("h8").orderBy("anillo", "wpi_id")
    area = (
        spark.table(T_PUERTOS)
        .withColumn("centro", dbf.h3_longlatash3("lon", "lat", config.PUERTO_RES_H3))
        .withColumn("h8", f.explode(dbf.h3_kring("centro", config.PUERTO_K_ANILLOS)))
        .withColumn("anillo", dbf.h3_distance("centro", "h8"))
        .withColumn("_rn", f.row_number().over(w_celda))
        .filter(f.col("_rn") == 1)
        .select("h8", "wpi_id")
    )
    en_puerto = (
        f.col("wpi_id").isNotNull()
        & (f.col("Status").isin(*config.EN_PUERTO_STATUS) | (f.col("SOG") < config.EN_PUERTO_SOG_MAX))
    )
    eventos = (
        spark.table(T_PUNTOS).filter(f.col("fecha").isin(FECHAS_CARGA))
        .withColumn("h8", dbf.h3_longlatash3("LON", "LAT", config.PUERTO_RES_H3))
        .join(f.broadcast(area), "h8", "left")
        .withColumn("puerto", f.when(f.coalesce(en_puerto, f.lit(False)), f.col("wpi_id")))
        .withColumn("prev_puerto", f.lag("puerto").over(w_buque))
        .withColumn("tiene_prev", f.lag("BaseDateTime").over(w_buque).isNotNull())
        .withColumn("arribo", f.col("puerto").isNotNull() & f.col("tiene_prev")
                    & (f.col("prev_puerto").isNull() | (f.col("prev_puerto") != f.col("puerto"))))
        .withColumn("presente_al_inicio", f.col("puerto").isNotNull() & ~f.col("tiene_prev"))
        .withColumn("zarpe_de", f.when(
            f.col("prev_puerto").isNotNull()
            & (f.col("puerto").isNull() | (f.col("puerto") != f.col("prev_puerto"))),
            f.col("prev_puerto")))
        .withColumn("horas_aporte", f.when(
            (f.col("puerto") == f.col("prev_puerto")) & (f.col("horas_desde_prev") <= 1),
            f.col("horas_desde_prev")).otherwise(0.0))
        .filter(f.col("fecha").isin(FECHAS))
    )
    llave = ["fecha", "wpi_id", "MMSI_Real", "MMSI"]
    en_puerto_dia = (
        eventos.filter(f.col("puerto").isNotNull())
        .groupBy("fecha", f.col("puerto").alias("wpi_id"), "MMSI_Real", "MMSI")
        .agg(
            f.max_by("VesselType", "BaseDateTime").alias("VesselType"),
            f.min("BaseDateTime").alias("primera_ts"),
            f.max("BaseDateTime").alias("ultima_ts"),
            f.count("*").alias("posiciones"),
            f.sum("horas_aporte").alias("horas_en_puerto"),
            f.count(f.when(f.col("arribo"), 1)).alias("arribos"),
            f.max("presente_al_inicio").alias("presente_al_inicio"),
        )
    )
    zarpes_dia = (
        eventos.filter(f.col("zarpe_de").isNotNull())
        .groupBy("fecha", f.col("zarpe_de").alias("wpi_id"), "MMSI_Real", "MMSI")
        .agg(f.count("*").alias("zarpes"), f.max_by("VesselType", "BaseDateTime").alias("vt_zarpe"))
    )
    puertos = spark.table(T_PUERTOS).select("wpi_id", "nombre_puerto", "pais")
    presencia = (
        en_puerto_dia.join(zarpes_dia, llave, "full")
        .withColumn("VesselType", f.coalesce("VesselType", "vt_zarpe"))
        .fillna({"posiciones": 0, "horas_en_puerto": 0.0, "arribos": 0, "zarpes": 0, "presente_al_inicio": False})
        .join(f.broadcast(puertos), "wpi_id", "left")
        .join(versiones_df, "fecha")
        .select(
            "fecha", f.col("wpi_id").cast("int").alias("wpi_id"), "nombre_puerto", "pais",
            "MMSI_Real", "MMSI", "VesselType", "primera_ts", "ultima_ts", "posiciones",
            "horas_en_puerto", "arribos", "zarpes", "presente_al_inicio", "_version_silver",
        )
    )
    escribir_fechas(presencia, T_PRESENCIA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Vista `visitas_puerto`
# MAGIC
# MAGIC Une los días de presencia de un mismo buque en un mismo puerto en visitas (*gaps and
# MAGIC islands*): cada arribo (o la presencia al inicio de los datos) abre una visita nueva, que
# MAGIC acumula los días siguientes hasta el próximo arribo. Al ser vista, se calcula al consultarla
# MAGIC y nunca queda desactualizada. Limitación: dos visitas al mismo puerto en el mismo día se
# MAGIC cuentan como una.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {V_VISITAS}
COMMENT 'Visitas completas a puerto reconstruidas desde presencia_puertos (una fila por visita).'
AS
WITH marcada AS (
    SELECT *,
           SUM(CASE WHEN arribos > 0 OR presente_al_inicio THEN 1 ELSE 0 END)
               OVER (PARTITION BY MMSI_Real, wpi_id ORDER BY fecha ROWS UNBOUNDED PRECEDING) AS visita_n
    FROM {T_PRESENCIA}
)
SELECT MMSI_Real, MMSI, wpi_id, nombre_puerto, pais, visita_n,
       MIN(primera_ts)                    AS entrada,
       MAX(ultima_ts)                     AS ultimo_reporte_en_puerto,
       MAX(zarpes) > 0                    AS cerrada,
       SUM(horas_en_puerto)               AS horas_en_puerto,
       COUNT(*)                           AS dias,
       BOOL_OR(presente_al_inicio)        AS empezo_antes_de_los_datos
FROM marcada
GROUP BY MMSI_Real, MMSI, wpi_id, nombre_puerto, pais, visita_n
""")
print(f"Vista {V_VISITAS} creada.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Evidencia
# MAGIC
# MAGIC ## Estado de versiones: oro al día con plata
# MAGIC
# MAGIC Después de correr, ninguna fecha debe quedar pendiente: cada tabla de oro tiene la misma
# MAGIC `_version_silver` que `calidad_metricas` para cada fecha. Correr el notebook de nuevo sin
# MAGIC cambios en plata no recalcula nada.

# COMMAND ----------

verificacion = version_silver
for i, t in enumerate(TABLAS_ORO):
    verificacion = verificacion.join(
        spark.table(t).groupBy("fecha").agg(f.max("_version_silver").alias(f"v{i}")), "fecha", "left")
atrasadas = verificacion.filter(
    " OR ".join(f"v{i} IS NULL OR v{i} < v_silver" for i in range(len(TABLAS_ORO)))
).count()
print(f"Fechas de oro atrasadas respecto a plata: {atrasadas}")
assert atrasadas == 0, "Alguna tabla de oro quedó desactualizada"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consistencia con plata
# MAGIC
# MAGIC - `trayectorias_puntos` tiene exactamente las mismas filas que plata para cada fecha.
# MAGIC - La suma de `puntos` de `trayectorias_diarias` es igual a las filas de `trayectorias_puntos`.

# COMMAND ----------

if FECHAS:
    filas = lambda t, col="fecha": spark.table(t).filter(f.col(col).isin(FECHAS)).count()
    n_silver, n_puntos = filas(T_SILVER), filas(T_PUNTOS)
    n_diarias = spark.table(T_DIARIAS).filter(f.col("fecha").isin(FECHAS)).agg(f.sum("puntos")).first()[0]
    print(f"plata {n_silver:,} · puntos {n_puntos:,} · suma de puntos por viaje {n_diarias:,}")
    assert n_silver == n_puntos == n_diarias, "Oro no cuadra con plata"

    print("Resumen por fecha:")
    (
        spark.table(T_DIARIAS).filter(f.col("fecha").isin(FECHAS)).groupBy("fecha")
        .agg(f.count_distinct("MMSI").alias("buques"), f.count("*").alias("viajes"),
             f.round(f.sum("distancia_nm")).alias("millas"),
             f.count(f.col("continua_de_fecha")).alias("viajes_que_siguen_de_la_vispera"),
             f.sum("anomalias_velocidad").alias("anomalias_velocidad"))
        .join(spark.table(T_PRESENCIA).filter(f.col("fecha").isin(FECHAS)).groupBy("fecha")
              .agg(f.count_distinct("wpi_id").alias("puertos_con_buques"), f.sum("arribos").alias("arribos")),
              "fecha", "left")
        .orderBy("fecha").show(truncate=False)
    )

# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 · ML: detección de anomalías de velocidad por viaje (MLflow)
# MAGIC
# MAGIC **OceanWatch Analytics · Entrega 2** · Última tarea del job · Requisito 6.
# MAGIC
# MAGIC ## Propósito
# MAGIC
# MAGIC Demostrar ML **dentro del pipeline reproducible**: características extraídas del lakehouse,
# MAGIC experimento trackeado en MLflow, modelo registrado en Unity Catalog y usado todos los días por
# MAGIC el job. No se busca maximizar la precisión.
# MAGIC
# MAGIC ## Entradas y salidas
# MAGIC
# MAGIC | | Objeto | Detalle |
# MAGIC |---|---|---|
# MAGIC | **Entrada** | `gold.trayectorias_diarias` | Un viaje por fila: velocidades, distancia, duración |
# MAGIC | **Entrada** | `silver.calidad_metricas` | Versión de plata por fecha (qué puntuar) |
# MAGIC | **Salida** | Experimento MLflow `/Users/<usuario>/oceanwatch_anomalias_velocidad` | Parámetros, métricas y modelo |
# MAGIC | **Salida** | Modelo `gold.modelo_anomalias_velocidad` (alias `@champion`) | Registrado en Unity Catalog |
# MAGIC | **Salida** | `gold.ml_anomalias_velocidad` | Puntaje de anomalía por viaje y día |
# MAGIC
# MAGIC **Parámetros:** `catalogo`; `reentrenar` (`false`; `true` entrena una versión nueva y la deja
# MAGIC como `@champion`); `recalcular_todo` (`false`; `true` vuelve a puntuar todas las fechas).
# MAGIC
# MAGIC ## Decisiones
# MAGIC
# MAGIC - **Modelo no supervisado (IsolationForest).** No hay etiquetas de "viaje anómalo"; el bosque
# MAGIC   aísla los viajes cuya combinación de velocidades es rara. Propuesta original del equipo,
# MAGIC   adaptada al esquema de oro y al patrón idempotente del pipeline.
# MAGIC - **Grano: un viaje** (`MMSI_Real`, `fecha`, `Trip_ID`), con características explícitas
# MAGIC   (`config.ML_CARACTERISTICAS`): velocidad implícita máxima, velocidad media del viaje, SOG
# MAGIC   promedio, distancia, duración y puntos. Solo viajes con 5 o más reportes y duración > 0:
# MAGIC   con menos, las velocidades no son confiables.
# MAGIC - **Entrenar solo cuando hace falta:** si no existe `@champion` o con `reentrenar = true`.
# MAGIC   La corrida diaria solo puntúa. Si hay más de 300 000 viajes, la muestra de entrenamiento es
# MAGIC   determinística (hash de la llave), igual que la semilla del bosque: mismo historial, mismo
# MAGIC   modelo.
# MAGIC - **Puntuar con el patrón de oro:** fechas cuya versión de plata cambió
# MAGIC   (`comun.fechas_pendientes`), más las puntuadas con otra versión del modelo; escritura con
# MAGIC   `replaceWhere` por fecha. Correr dos veces no cambia nada.
# MAGIC - **Referencia (baseline): la regla de `04_gold`**, `anomalias_velocidad > 0` (algún salto a
# MAGIC   más de 60 nudos). Se mide qué parte de lo que marca el modelo coincide con la regla y cuántas
# MAGIC   anomalías encuentra que la regla no ve.

# COMMAND ----------

import math

import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
from mlflow.models import infer_signature
from pyspark.sql import functions as f
from sklearn.ensemble import IsolationForest

import comun
import config

spark = config.obtener_spark()
spark.conf.set("spark.sql.session.timeZone", "UTC")

CATALOGO = config.leer_parametro("catalogo", config.CATALOGO_DEFECTO)
REENTRENAR = config.leer_parametro("reentrenar", "false").lower() == "true"
RECALCULAR_TODO = config.leer_parametro("recalcular_todo", "false").lower() == "true"
cfg = config.Config(CATALOGO)

T_METRICAS = cfg.tabla("silver", "calidad_metricas")
T_DIARIAS = cfg.tabla("gold", "trayectorias_diarias")
T_ML = cfg.tabla("gold", "ml_anomalias_velocidad")
MODELO = cfg.tabla("gold", config.ML_MODELO)
CARACT = config.ML_CARACTERISTICAS

mlflow.set_registry_uri("databricks-uc")
usuario = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{usuario}/{config.ML_EXPERIMENTO}")
cliente = MlflowClient()
print(f"{T_DIARIAS}  ->  {MODELO} @champion  ->  {T_ML}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Características desde el lakehouse

# COMMAND ----------

def caracteristicas(df):
    """Un viaje por fila con sus características numéricas y la marca de la regla (baseline)."""
    return (
        df.filter((f.col("puntos") >= config.ML_MIN_PUNTOS) & (f.col("duracion_h") > 0))
        .withColumn("velocidad_media_kn", f.col("distancia_nm") / f.col("duracion_h"))
        .withColumn("anomalia_regla", f.col("anomalias_velocidad") > 0)
        .select(
            "fecha", "MMSI_Real", "MMSI", "Trip_ID", "VesselType", "anomalia_regla",
            *[f.coalesce(f.col(c).cast("double"), f.lit(0.0)).alias(c) for c in CARACT],
        )
    )


def version_champion() -> str | None:
    try:
        return cliente.get_model_version_by_alias(MODELO, "champion").version
    except Exception:
        return None


VERSION = version_champion()
ENTRENAR = REENTRENAR or VERSION is None
print(f"Modelo @champion: {VERSION or 'no existe'} · entrenar en esta corrida: {ENTRENAR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Entrenamiento (solo si hace falta)
# MAGIC
# MAGIC Todo el historial de viajes, con semilla fija. Métricas registradas en MLflow:
# MAGIC
# MAGIC | Métrica | Qué dice |
# MAGIC |---|---|
# MAGIC | `tasa_anomalias_pct` | % de viajes que el modelo marca (≈ contaminación) |
# MAGIC | `coincidencia_con_regla_pct` | De lo que marca el modelo, % que también marca la regla de 60 nudos |
# MAGIC | `cobertura_de_regla_pct` | De lo que marca la regla, % que el modelo también encuentra |
# MAGIC | `anomalias_nuevas` | Viajes que el modelo marca y la regla no: lo que aporta el modelo |

# COMMAND ----------

if ENTRENAR:
    historial = caracteristicas(spark.table(T_DIARIAS))
    n_total = historial.count()
    paso = max(1, math.ceil(n_total / config.ML_MAX_ENTRENAMIENTO))
    if paso > 1:  # muestra determinística: misma llave, misma decisión en cada corrida
        historial = historial.filter(f.abs(f.xxhash64("MMSI_Real", "fecha", "Trip_ID")) % paso == 0)
    pdf = historial.toPandas()
    X = pdf[CARACT]

    with mlflow.start_run(run_name="isolation_forest_viajes") as corrida:
        mlflow.log_params({
            "algoritmo": "IsolationForest",
            "n_estimators": config.ML_ARBOLES,
            "contamination": config.ML_CONTAMINACION,
            "random_state": config.ML_SEMILLA,
            "caracteristicas": ",".join(CARACT),
            "min_puntos": config.ML_MIN_PUNTOS,
            "viajes_entrenamiento": len(pdf),
            "fechas": f"{pdf['fecha'].min()} … {pdf['fecha'].max()}",
        })
        modelo = IsolationForest(
            n_estimators=config.ML_ARBOLES,
            contamination=config.ML_CONTAMINACION,
            random_state=config.ML_SEMILLA,
        ).fit(X)

        marca = modelo.predict(X) == -1
        regla = pdf["anomalia_regla"].to_numpy()
        ambas = int((marca & regla).sum())
        mlflow.log_metrics({
            "viajes": len(pdf),
            "tasa_anomalias_pct": round(100 * marca.mean(), 3),
            "coincidencia_con_regla_pct": round(100 * ambas / max(int(marca.sum()), 1), 2),
            "cobertura_de_regla_pct": round(100 * ambas / max(int(regla.sum()), 1), 2),
            "anomalias_nuevas": int((marca & ~regla).sum()),
        })

        info = mlflow.sklearn.log_model(
            sk_model=modelo,
            artifact_path="modelo",
            signature=infer_signature(X, modelo.decision_function(X)),
            input_example=X.head(5),
            registered_model_name=MODELO,
        )
        VERSION = str(info.registered_model_version)
        cliente.set_registered_model_alias(MODELO, "champion", VERSION)
        print(f"Run {corrida.info.run_id}: {len(pdf):,} viajes · versión {VERSION} → @champion")
else:
    print("Se usa el @champion existente; esta corrida solo puntúa.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Puntuación diaria (idempotente por fecha)
# MAGIC
# MAGIC `score_anomalia` es el `decision_function` del bosque: **más bajo = más anómalo**.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_ML} (
    fecha                       DATE      NOT NULL COMMENT 'Fecha UTC del viaje',
    MMSI_Real                   STRING    NOT NULL COMMENT 'MMSI + H3_Macro',
    MMSI                        STRING    NOT NULL COMMENT 'Identificador del buque',
    Trip_ID                     INT       NOT NULL COMMENT 'Viaje dentro de (MMSI_Real, fecha)',
    VesselType                  INT                COMMENT 'Código AIS de tipo de buque',
    velocidad_implicita_max_kn  DOUBLE             COMMENT 'Característica: salto más rápido del viaje',
    velocidad_media_kn          DOUBLE             COMMENT 'Característica: distancia / duración',
    sog_promedio                DOUBLE             COMMENT 'Característica: SOG reportado promedio (0 si no hubo)',
    distancia_nm                DOUBLE             COMMENT 'Característica: distancia del viaje',
    duracion_h                  DOUBLE             COMMENT 'Característica: duración del viaje',
    puntos                      DOUBLE             COMMENT 'Característica: reportes del viaje',
    score_anomalia              DOUBLE    NOT NULL COMMENT 'decision_function del IsolationForest: más bajo = más anómalo',
    es_anomalia_ml              BOOLEAN   NOT NULL COMMENT 'El modelo marca el viaje como anómalo',
    anomalia_regla              BOOLEAN   NOT NULL COMMENT 'Baseline: la regla de 04_gold (salto > 60 nudos) marca el viaje',
    _version_modelo             STRING    NOT NULL COMMENT 'Versión del modelo en Unity Catalog con que se puntuó',
    _version_silver             TIMESTAMP NOT NULL COMMENT '_calculado_ts de plata con que se construyó esta fecha',
    CONSTRAINT pk_ml_anomalias_velocidad PRIMARY KEY (fecha, MMSI_Real, Trip_ID)
)
CLUSTER BY (fecha)
COMMENT 'Puntaje de anomalía de velocidad por viaje (IsolationForest registrado en {MODELO}). Requisito 6.'
""")

FECHAS, VERSIONES = comun.fechas_pendientes(spark, T_METRICAS, [T_ML], RECALCULAR_TODO or ENTRENAR)
# También las fechas puntuadas con otra versión del modelo (p. ej. si se reentrenó en otra corrida).
otra_version = {
    r.fecha for r in spark.table(T_ML).filter(f.col("_version_modelo") != VERSION)
    .select("fecha").distinct().collect()
}
FECHAS = sorted(set(FECHAS) | (otra_version & set(VERSIONES)))
print(f"Fechas por puntuar: {[str(d) for d in FECHAS] or 'ninguna'}")

if FECHAS:
    modelo = mlflow.sklearn.load_model(f"models:/{MODELO}@champion")
    pdf = caracteristicas(spark.table(T_DIARIAS).filter(f.col("fecha").isin(FECHAS))).toPandas()
    pdf["score_anomalia"] = modelo.decision_function(pdf[CARACT])
    pdf["es_anomalia_ml"] = modelo.predict(pdf[CARACT]) == -1
    pdf["_version_modelo"] = VERSION
    puntuado = (
        spark.createDataFrame(pdf)
        .select(
            "fecha", "MMSI_Real", "MMSI", f.col("Trip_ID").cast("int").alias("Trip_ID"),
            f.col("VesselType").cast("int").alias("VesselType"), *CARACT,
            "score_anomalia", "es_anomalia_ml", "anomalia_regla", "_version_modelo",
        )
        .join(comun.df_versiones(spark, FECHAS, VERSIONES), "fecha")
    )
    comun.escribir_fechas(spark, puntuado, T_ML, FECHAS)

# COMMAND ----------

# MAGIC %md
# MAGIC # Evidencia
# MAGIC
# MAGIC ## Modelo contra la regla, por fecha

# COMMAND ----------

(
    spark.table(T_ML).groupBy("fecha")
    .agg(
        f.count("*").alias("viajes"),
        f.sum(f.col("es_anomalia_ml").cast("int")).alias("anomalias_ml"),
        f.sum(f.col("anomalia_regla").cast("int")).alias("anomalias_regla"),
        f.sum((f.col("es_anomalia_ml") & f.col("anomalia_regla")).cast("int")).alias("ambas"),
        f.sum((f.col("es_anomalia_ml") & ~f.col("anomalia_regla")).cast("int")).alias("solo_ml"),
        f.max("_version_modelo").alias("modelo"),
    )
    .orderBy("fecha").show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Los viajes más anómalos que la regla no marca

# COMMAND ----------

(
    spark.table(T_ML).filter(f.col("es_anomalia_ml") & ~f.col("anomalia_regla"))
    .orderBy("score_anomalia")
    .select("fecha", "MMSI", "Trip_ID", "VesselType", f.round("score_anomalia", 3).alias("score"),
            *[f.round(c, 1).alias(c) for c in CARACT])
    .show(10, truncate=False)
)

"""Funciones compartidas del pipeline OceanWatch (Entrega 2).

Las usan la compuerta de lote (03b) y las tareas de oro (04a-04e) para que todas decidan con la
misma lógica qué fechas procesar y escriban de la misma forma. Igual que config.py, se importa
desde un notebook del mismo directorio:

    import comun
    fechas, versiones = comun.fechas_pendientes(spark, T_METRICAS, [T_PUNTOS, T_DIARIAS])
"""

from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as f


# ─── Qué fechas recalcular (sección 2 de 04_gold, sin cambios de lógica) ───

def fechas_pendientes(spark, t_metricas: str, tablas: list[str], recalcular_todo: bool = False):
    """Fechas que hay que (re)calcular en `tablas` y la versión de plata de cada fecha.

    Una fecha está pendiente si alguna de las tablas no la tiene, o la construyó con una versión
    de plata (`_calculado_ts` de calidad_metricas) más vieja que la actual. Se agrega el día
    siguiente de cada fecha pendiente, si existe en plata: sus trayectorias y presencias
    dependen del último punto del día anterior.

    Si una tabla todavía no existe (primera corrida), todas las fechas de plata quedan pendientes.
    Si la tabla de métricas no existe, no hay nada que procesar.

    Devuelve (lista ordenada de fechas, {fecha: version_silver} con todas las fechas de plata).
    """
    if not spark.catalog.tableExists(t_metricas):
        return [], {}

    estado = spark.table(t_metricas).groupBy("fecha").agg(f.max("_calculado_ts").alias("v_silver"))
    existentes = [t for t in tablas if spark.catalog.tableExists(t)]
    for i, t in enumerate(existentes):
        estado = estado.join(
            spark.table(t).groupBy("fecha").agg(f.max("_version_silver").alias(f"v{i}")),
            "fecha",
            "left",
        )

    pendiente = f.lit(recalcular_todo or len(existentes) < len(tablas))
    for i in range(len(existentes)):
        pendiente = pendiente | f.col(f"v{i}").isNull() | (f.col(f"v{i}") < f.col("v_silver"))

    filas = estado.select("fecha", "v_silver", pendiente.alias("pendiente")).collect()
    versiones = {r.fecha: r.v_silver for r in filas}
    base = {r.fecha for r in filas if r.pendiente}
    siguientes = {d + timedelta(days=1) for d in base if d + timedelta(days=1) in versiones}
    return sorted(base | siguientes), versiones


def df_versiones(spark, fechas: list, versiones: dict):
    """DataFrame (fecha, _version_silver) para unir a lo que se va a escribir."""
    return spark.createDataFrame(
        [(d, versiones[d]) for d in fechas], "fecha DATE, _version_silver TIMESTAMP"
    )


# ─── Escritura idempotente por fecha (la de 04_gold, sin variables globales) ───

def escribir_fechas(spark, df, tabla: str, fechas: list) -> int:
    """Sobrescribe solo las fechas dadas: estado completo de esas fechas, atómico e idempotente.

    Devuelve cuántas filas quedaron en esas fechas.
    """
    if not fechas:
        return 0
    lista = ", ".join(f"DATE'{d}'" for d in fechas)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"fecha IN ({lista})")
        .saveAsTable(tabla)
    )
    n = spark.table(tabla).filter(f.col("fecha").isin(fechas)).count()
    print(f"  {tabla}: {n:,} filas en {len(fechas)} fecha(s)")
    return n


# ─── Monitoreo: una fila por tarea y corrida en ops.ejecuciones (se usa desde el paso 6) ───

ESQUEMA_EJECUCIONES = (
    "run_id STRING, tarea STRING, fechas ARRAY<DATE>, filas BIGINT, inicio TIMESTAMP, "
    "fin TIMESTAMP, duracion_s DOUBLE, estado STRING, detalle STRING"
)


def ahora() -> datetime:
    """Marca de tiempo UTC para el inicio de una tarea."""
    return datetime.now(timezone.utc)


def registrar_ejecucion(spark, tabla: str, run_id: str, tarea: str, fechas: list, filas: int,
                        inicio: datetime, estado: str, detalle: str | None = None) -> None:
    """Agrega una fila a la tabla de ejecuciones (es un log: solo append)."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabla} ({ESQUEMA_EJECUCIONES})
        COMMENT 'Una fila por tarea del job y corrida: fechas procesadas, filas, duración y estado.'
    """)
    fin = ahora()
    fila = [(
        run_id or "manual", tarea, list(fechas or []), int(filas or 0),
        inicio, fin, (fin - inicio).total_seconds(), estado, detalle,
    )]
    spark.createDataFrame(fila, ESQUEMA_EJECUCIONES).write.mode("append").saveAsTable(tabla)
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 · Productor: simulador del feed diario AIS
# MAGIC
# MAGIC **OceanWatch Analytics · Entrega 2** · Tarea 2 del job · Mapa completo del pipeline en el
# MAGIC [README](../README.md#entrega-2--pipeline-en-streaming).
# MAGIC
# MAGIC ## Propósito
# MAGIC
# MAGIC Simular la llegada del "feed diario de archivos" del enunciado: deja en
# MAGIC `landing/ais/entrantes/` el CSV de posiciones de un día (o de un rango, para backfill), tal
# MAGIC como lo publica NOAA. Este notebook **es la fuente**, no parte del pipeline: el pipeline
# MAGIC empieza en bronce, que lee lo que este deja.
# MAGIC
# MAGIC ## Entradas y salidas
# MAGIC
# MAGIC | | Objeto | Detalle |
# MAGIC |---|---|---|
# MAGIC | **Entrada** | NOAA MarineCadastre | `AIS_YYYY_MM_DD.zip` (~350 MB), un CSV de ~900 MB adentro |
# MAGIC | **Salida** | `landing/ais/_descargas/` | El zip original (copia cruda y caché) |
# MAGIC | **Salida** | `landing/ais/entrantes/` | `AIS_YYYY_MM_DD.csv`, un archivo completo por día |
# MAGIC | **Salida (opcional)** | `landing/ais/entrantes/` | `…_reenvio.csv` y `…_inyectados.csv` para las demostraciones |
# MAGIC
# MAGIC **Parámetros**
# MAGIC
# MAGIC | Parámetro | Por defecto | Uso |
# MAGIC |---|---|---|
# MAGIC | `catalogo` | `oceanwatch_lakehouse` | Catálogo destino |
# MAGIC | `fecha` | vacío → reloj simulado | Día a entregar (`YYYY-MM-DD`) |
# MAGIC | `fecha_fin` | vacío | Si se da, entrega el rango `fecha … fecha_fin` (backfill) |
# MAGIC | `reenviar` | `false` | Además entrega una copia del día con otro nombre (duplicados) |
# MAGIC | `inyectar_invalidos` | `false` | Además entrega un archivo pequeño con una fila por motivo de cuarentena |
# MAGIC
# MAGIC ## Decisiones
# MAGIC
# MAGIC - **Un archivo completo por día**, igual que el parcial y que la fuente: el CSV de NOAA
# MAGIC   entra intacto. Los datos tardíos se simulan entregando un día fuera de orden (o un
# MAGIC   reenvío): plata los absorbe con `MERGE` y oro recalcula la fecha afectada.
# MAGIC - **Atomicidad por subida única.** Los Volumes viven en almacenamiento de objetos: no hay
# MAGIC   "renombrar" (un `mv` es copiar + borrar), pero **subir un objeto es atómico**: quien lo lee
# MAGIC   lo ve completo o no lo ve. El archivo se arma en el disco local del driver (descarga +
# MAGIC   descompresión) y se sube de una sola vez a `entrantes/`; Auto Loader nunca ve un archivo
# MAGIC   a medias.
# MAGIC - **Nombres inmutables = productor idempotente.** Si `AIS_YYYY_MM_DD.csv` ya está en
# MAGIC   `entrantes/`, no se vuelve a subir. Correr el productor dos veces para el mismo día no
# MAGIC   tiene efecto; un reintento del job tampoco.
# MAGIC - **Reloj simulado.** Los datos son de 2023 y el job corre en 2026. Sin `fecha`, se entrega
# MAGIC   `DIA_CERO_DATOS + (hoy − INICIO_SIMULACION)`: cada día real, un día distinto de datos; dos
# MAGIC   corridas el mismo día real entregan el mismo archivo (y la segunda no hace nada).
# MAGIC - **Anomalías en archivos aparte, nunca mezcladas con los datos reales.** Los duplicados
# MAGIC   reales son ~0,003 % y no alcanzan para demostrar la deduplicación ni la cuarentena; por eso
# MAGIC   se pueden inyectar, pero en archivos con nombre propio: bronce guarda el archivo de origen
# MAGIC   de cada fila, así que lo inyectado siempre es identificable.

# COMMAND ----------

import csv
import os
import tempfile
import time
import zipfile
from datetime import date, datetime, timedelta, timezone

import requests
from databricks.sdk.errors import NotFound

import config

# El productor solo mueve archivos: no necesita Spark, solo la Files API del SDK.
w = config.obtener_workspace()

CATALOGO = config.leer_parametro("catalogo", config.CATALOGO_DEFECTO)
FECHA = config.leer_parametro("fecha", "")
FECHA_FIN = config.leer_parametro("fecha_fin", "")
REENVIAR = config.leer_parametro("reenviar", "false").lower() == "true"
INYECTAR_INVALIDOS = config.leer_parametro("inyectar_invalidos", "false").lower() == "true"

cfg = config.Config(CATALOGO)
RUTA_DESCARGAS = cfg.volume("landing", "ais") + "/_descargas"
RUTA_ENTRANTES = cfg.ruta_ais_entrantes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Qué días entregar

# COMMAND ----------

def a_fecha(texto: str) -> date:
    return datetime.strptime(texto, "%Y-%m-%d").date()


def fecha_por_reloj_simulado() -> date:
    hoy = datetime.now(timezone.utc).date()
    dias = (hoy - a_fecha(config.INICIO_SIMULACION)).days
    return a_fecha(config.DIA_CERO_DATOS) + timedelta(days=dias)


if FECHA:
    inicio = a_fecha(FECHA)
    fin = a_fecha(FECHA_FIN) if FECHA_FIN else inicio
    origen = "parámetro"
else:
    inicio = fin = fecha_por_reloj_simulado()
    origen = "reloj simulado"

assert fin >= inicio, f"fecha_fin ({fin}) es anterior a fecha ({inicio})"
dias = [inicio + timedelta(days=i) for i in range((fin - inicio).days + 1)]
print(f"Días a entregar ({origen}): {dias[0]} … {dias[-1]} ({len(dias)} días)")
print(f"Reenviar: {REENVIAR} · Inyectar inválidos: {INYECTAR_INVALIDOS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Utilidades
# MAGIC
# MAGIC La descarga y la verificación de integridad (`testzip`, que revisa el CRC de cada archivo del
# MAGIC zip) son las de la Entrega 1. Lo nuevo: el zip se guarda en `_descargas/` como copia cruda,
# MAGIC y si ya está ahí se reutiliza en vez de volver a pedirlo a NOAA.

# COMMAND ----------

def existe(ruta: str) -> bool:
    try:
        w.files.get_metadata(ruta)
        return True
    except NotFound:
        return False


def zip_ok(ruta_local: str) -> bool:
    """True si es un zip válido y todos sus CRC coinciden."""
    if not zipfile.is_zipfile(ruta_local):
        return False
    with zipfile.ZipFile(ruta_local) as z:
        return z.testzip() is None


def descargar_noaa(d: date, destino: str, reintentos: int = 3) -> None:
    url = f"{config.URL_BASE_NOAA}/{d.year}/AIS_{d:%Y_%m_%d}.zip"
    for intento in range(1, reintentos + 1):
        try:
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(destino, "wb") as fh:
                    for bloque in r.iter_content(chunk_size=8 * 1024 * 1024):
                        fh.write(bloque)
            if not zip_ok(destino):
                raise ValueError("zip descargado pero corrupto")
            return
        except (requests.RequestException, ValueError) as e:
            print(f"  intento {intento}/{reintentos} fallido: {e}")
            if intento == reintentos:
                raise
            time.sleep(10 * intento)


def obtener_zip(d: date, tmp: str) -> str:
    """Deja el zip del día en el disco local; lo toma de _descargas/ si ya existe, si no de NOAA."""
    nombre = f"AIS_{d:%Y_%m_%d}.zip"
    local, remoto = os.path.join(tmp, nombre), f"{RUTA_DESCARGAS}/{nombre}"
    if existe(remoto):
        w.files.download_to(remoto, local)
        if zip_ok(local):
            print(f"  zip desde _descargas/ ({os.path.getsize(local) / 1e6:,.0f} MB)")
            return local
        print("  zip en _descargas/ corrupto: se descarga de nuevo")
    descargar_noaa(d, local)
    w.files.upload_from(remoto, local, overwrite=True)
    print(f"  zip desde NOAA ({os.path.getsize(local) / 1e6:,.0f} MB), guardado en _descargas/")
    return local


def extraer_csv(ruta_zip: str, tmp: str) -> str:
    with zipfile.ZipFile(ruta_zip) as z:
        miembros = [m for m in z.namelist() if m.lower().endswith(".csv")]
        assert len(miembros) == 1, f"Se esperaba 1 CSV en el zip, hay {miembros}"
        return z.extract(miembros[0], tmp)


def subir_si_no_existe(local: str, nombre: str) -> bool:
    """Subida única (atómica) a entrantes/. overwrite=False: el nombre es inmutable."""
    remoto = f"{RUTA_ENTRANTES}/{nombre}"
    if existe(remoto):
        print(f"  {nombre}: ya está en entrantes/, no se sube (idempotente)")
        return False
    w.files.upload_from(remoto, local, overwrite=False)
    print(f"  {nombre}: entregado ({os.path.getsize(local) / 1e6:,.0f} MB)")
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Filas inválidas inyectadas
# MAGIC
# MAGIC Se toma la primera fila real del día como base y se le altera **un solo campo** por fila,
# MAGIC para que cada una tenga exactamente un motivo. `VesselName` dice qué se espera de la fila,
# MAGIC así plata puede verificar que cada una terminó donde debía.
# MAGIC
# MAGIC | `VesselName` | Campo alterado | Destino esperado en plata |
# MAGIC |---|---|---|
# MAGIC | `INYECTADO_lat_fuera_de_rango` | `LAT = 95` | cuarentena |
# MAGIC | `INYECTADO_lon_fuera_de_rango` | `LON = -200` | cuarentena |
# MAGIC | `INYECTADO_coordenada_nula` | `LAT` vacío | cuarentena |
# MAGIC | `INYECTADO_en_tierra` | punto en Kansas | cuarentena |
# MAGIC | `INYECTADO_mmsi_mal_formado` | `MMSI = 12AB5` | cuarentena |
# MAGIC | `INYECTADO_mmsi_nulo` | `MMSI` vacío | cuarentena |
# MAGIC | `INYECTADO_timestamp_ilegible` | hora `25:61` | cuarentena |
# MAGIC | `INYECTADO_timestamp_nulo` | `BaseDateTime` vacío | cuarentena |
# MAGIC | `CONTROL_sog_no_disponible` | `SOG = 102.3` | **plata** (regla suave: SOG a nulo, la fila sigue) |
# MAGIC
# MAGIC La última fila es un **control negativo**: comprueba que una anomalía suave *no* manda la fila
# MAGIC a cuarentena.

# COMMAND ----------

def escribir_inyectados(csv_real: str, d: date, destino: str) -> int:
    with open(csv_real, newline="", encoding="utf-8") as fh:
        lector = csv.DictReader(fh)
        encabezado, base = lector.fieldnames, next(lector)

    alteraciones = [
        ("INYECTADO_lat_fuera_de_rango", {"LAT": "95.0"}),
        ("INYECTADO_lon_fuera_de_rango", {"LON": "-200.0"}),
        ("INYECTADO_coordenada_nula", {"LAT": ""}),
        ("INYECTADO_en_tierra", {"LAT": "38.5", "LON": "-98.5"}),
        ("INYECTADO_mmsi_mal_formado", {"MMSI": "12AB5"}),
        ("INYECTADO_mmsi_nulo", {"MMSI": ""}),
        ("INYECTADO_timestamp_ilegible", {"BaseDateTime": f"{d:%Y-%m-%d}T25:61:00"}),
        ("INYECTADO_timestamp_nulo", {"BaseDateTime": ""}),
        ("CONTROL_sog_no_disponible", {"SOG": "102.3"}),
    ]
    with open(destino, "w", newline="", encoding="utf-8") as fh:
        escritor = csv.DictWriter(fh, fieldnames=encabezado)
        escritor.writeheader()
        for i, (nombre, cambios) in enumerate(alteraciones):
            fila = dict(base)
            # Hora propia por fila (12:00:00, 12:00:01, …) para que ninguna sea duplicado de otra
            fila["BaseDateTime"] = f"{d:%Y-%m-%d}T12:00:{i:02d}"
            fila["VesselName"] = nombre
            fila.update(cambios)
            escritor.writerow(fila)
    return len(alteraciones)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Entrega

# COMMAND ----------

w.files.create_directory(RUTA_DESCARGAS)
w.files.create_directory(RUTA_ENTRANTES)

resumen = []
for d in dias:
    nombre = f"AIS_{d:%Y_%m_%d}.csv"
    extras = []
    if REENVIAR:
        extras.append(f"AIS_{d:%Y_%m_%d}_reenvio.csv")
    if INYECTAR_INVALIDOS:
        extras.append(f"AIS_{d:%Y_%m_%d}_inyectados.csv")

    print(f"{d}:")
    pendientes = [n for n in [nombre, *extras] if not existe(f"{RUTA_ENTRANTES}/{n}")]
    if not pendientes:
        print("  todo ya entregado, no se hace nada")
        resumen.append((str(d), 0))
        continue

    t0 = time.time()
    with tempfile.TemporaryDirectory() as tmp:  # el staging: disco local del driver
        csv_local = extraer_csv(obtener_zip(d, tmp), tmp)
        entregados = int(subir_si_no_existe(csv_local, nombre))
        if REENVIAR:
            entregados += subir_si_no_existe(csv_local, f"AIS_{d:%Y_%m_%d}_reenvio.csv")
        if INYECTAR_INVALIDOS:
            ruta_iny = os.path.join(tmp, f"AIS_{d:%Y_%m_%d}_inyectados.csv")
            n = escribir_inyectados(csv_local, d, ruta_iny)
            print(f"  {n} filas inyectadas")
            entregados += subir_si_no_existe(ruta_iny, f"AIS_{d:%Y_%m_%d}_inyectados.csv")
    print(f"  {entregados} archivo(s) entregado(s) en {time.time() - t0:,.0f} s")
    resumen.append((str(d), entregados))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidencia: estado de `entrantes/`

# COMMAND ----------

print(f"Entregados en esta corrida: {sum(n for _, n in resumen)} archivo(s)")
print(f"\nContenido de {RUTA_ENTRANTES}:")
for e in sorted(w.files.list_directory_contents(RUTA_ENTRANTES), key=lambda e: e.name):
    print(f"  {e.name:40s} {(e.file_size or 0) / 1e6:>8,.0f} MB")

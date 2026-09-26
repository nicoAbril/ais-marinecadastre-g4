# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 - Ingesta AIS (MarineCadastre)
# MAGIC
# MAGIC **Fuente:** datos AIS diarios de MarineCadastre (NOAA), 7 días: 2023-06-01 a 2023-06-07.
# MAGIC
# MAGIC **Contenido de este notebook:**
# MAGIC 1. Creación del catálogo, esquema y Volume del proyecto en Unity Catalog.
# MAGIC 2. Descarga de los zip diarios con reintentos y verificación de integridad.
# MAGIC 3. Descompresión de los CSV en el Volume.

# COMMAND ----------

import os
import time
import requests
import zipfile
from datetime import datetime, timedelta

# En Databricks `spark` ya existe; en local se crea con Databricks Connect
try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile("uniandes").serverless(True).getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración y gobernanza
# MAGIC
# MAGIC Todo el proyecto vive en el catálogo `oceanwatch`. El esquema `raw` contiene el Volume
# MAGIC `ais_files`, donde se guardan los archivos tal como se descargan:
# MAGIC `positions/zips/` con los zip originales, `positions/csvs/` con los CSV descomprimidos y
# MAGIC `reference/` con los catálogos de referencia (World Port Index).

# COMMAND ----------

# Definición variables de ejecucion
CATALOG = 'oceanwatch'
RAW_SCHEMA = 'raw'
AIS_VOLUME = 'ais_files'
POSITIONS_PATH = 'positions'
ZIP_PATH = os.path.join(POSITIONS_PATH, 'zips')
CSV_PATH = os.path.join(POSITIONS_PATH, 'csvs')

# COMMAND ----------

# Creación de catalogo del proyecto y volumen de volcado de datos crudos
spark.sql(
    f"""
    CREATE CATALOG IF NOT EXISTS {CATALOG}
    COMMENT 'Proyecto OceanWatch AIS'
    """
)
spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}
    COMMENT 'Datos Crudos'
    """
)

spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{AIS_VOLUME}
    COMMENT 'Datos crudos AIS: positions/zips (zip diarios), positions/csvs (CSV descomprimidos)
    y reference/ (World Port Index)'
    """
)

# El Volume puede ya existir de una ejecución anterior con el comentario viejo; COMMENT ON VOLUME
# lo actualiza sin recrearlo.
spark.sql(f"""
    COMMENT ON VOLUME {CATALOG}.{RAW_SCHEMA}.{AIS_VOLUME} IS
    'Datos crudos AIS: positions/zips (zip diarios), positions/csvs (CSV descomprimidos) y
    reference/ (World Port Index)'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Descarga y descompresión
# MAGIC
# MAGIC - `download_day`: descarga el zip de un día. Si el zip ya existe y es válido no lo vuelve a
# MAGIC   descargar. Reintenta hasta 3 veces (con espera creciente) ante errores de red o un zip corrupto.
# MAGIC - **Integridad:** `zipfile.testzip()` revisa el CRC de cada archivo del zip; si el zip está
# MAGIC   truncado o dañado se borra y se reintenta.
# MAGIC - `unzip_day`: extrae el CSV en `csvs/`; si ya existe, no hace nada.

# COMMAND ----------

# Descarga de datos y descompresión
VOLUME_BASE = os.path.join('/Volumes',CATALOG, RAW_SCHEMA, AIS_VOLUME)
ZIPS_DIR = os.path.join(VOLUME_BASE, ZIP_PATH)
CSV_DIR = os.path.join(VOLUME_BASE, CSV_PATH)
URL_BASE = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"

def zip_ok(path):
    """True si el archivo es un zip válido y todos sus CRC coinciden."""
    if not zipfile.is_zipfile(path):
        return False
    with zipfile.ZipFile(path) as z:
        return z.testzip() is None

def download_day(d, zips_dir = ZIPS_DIR, url_base = URL_BASE, retries = 3):
    file_name = f"AIS_{d.strftime('%Y_%m_%d')}.zip"
    source_url = os.path.join(url_base,str(d.year),file_name)
    dest_zip = os.path.join(zips_dir, file_name)
    os.makedirs(zips_dir, exist_ok=True)
    if os.path.exists(dest_zip) and zip_ok(dest_zip):
        return dest_zip
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(source_url, timeout=60)
            response.raise_for_status()
            with open(dest_zip, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            if not zip_ok(dest_zip):
                raise ValueError(f"{file_name} descargado pero corrupto")
            return dest_zip
        except (requests.RequestException, ValueError) as e:
            print(f"Intento {attempt}/{retries} fallido para {file_name}: {e}")
            if os.path.exists(dest_zip):
                os.remove(dest_zip)
            if attempt == retries:
                raise
            time.sleep(2 * attempt)

def unzip_day(d, zips_dir = ZIPS_DIR, csv_dir = CSV_DIR):
    zip_name = f"AIS_{d.strftime('%Y_%m_%d')}.zip"
    csv_name = f"AIS_{d.strftime('%Y_%m_%d')}.csv"
    source_zip = os.path.join(zips_dir, zip_name)
    dest_csv = os.path.join(csv_dir, csv_name)
    os.makedirs(csv_dir, exist_ok=True)
    if os.path.exists(dest_csv):
        return dest_csv
    with zipfile.ZipFile(source_zip, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(dest_csv))
    return dest_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución para el rango de fechas

# COMMAND ----------

start_date = datetime.strptime('2023-06-01', '%Y-%m-%d').date()
end_date = datetime.strptime('2023-06-07', '%Y-%m-%d').date()
n_days = (end_date-start_date).days+1
for i in range(n_days):
    d = start_date + timedelta(days=i)
    download_day(d)
    unzip_day(d)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Recurso de referencia: World Port Index
# MAGIC
# MAGIC Para la pregunta de negocio sobre concentración de tráfico se necesita cruzar las celdas
# MAGIC espaciales con los puertos conocidos. Se usa el **World Port Index** (NGA Pub. 150), que
# MAGIC publica la NGA en CSV con la posición y características de ~3 800 puertos del mundo.
# MAGIC
# MAGIC - **Fuente:** https://msi.nga.mil/Publications/WPI
# MAGIC - **Descarga directa:** https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv
# MAGIC
# MAGIC Se descarga con el mismo criterio de reintentos e idempotencia que los zip de AIS, en la
# MAGIC carpeta `reference/` del Volume.

# COMMAND ----------

REFERENCE_DIR = os.path.join(VOLUME_BASE, 'reference')
WPI_URL = "https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv"
WPI_PATH = os.path.join(REFERENCE_DIR, "world_port_index.csv")

def download_reference_file(url, dest_path, retries = 3):
    """Descarga un archivo de referencia (no zip) con reintentos. Si ya existe, no lo descarga de nuevo."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    if os.path.exists(dest_path):
        return dest_path
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            with open(dest_path, "wb") as f:
                f.write(response.content)
            if os.path.getsize(dest_path) == 0:
                raise ValueError(f"{os.path.basename(dest_path)} descargado vacío")
            return dest_path
        except (requests.RequestException, ValueError) as e:
            print(f"Intento {attempt}/{retries} fallido para {os.path.basename(dest_path)}: {e}")
            if os.path.exists(dest_path):
                os.remove(dest_path)
            if attempt == retries:
                raise
            time.sleep(2 * attempt)

download_reference_file(WPI_URL, WPI_PATH)
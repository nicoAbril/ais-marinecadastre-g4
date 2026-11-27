# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00 · Preparación: estructura del lakehouse y fuentes de referencia
# MAGIC
# MAGIC **OceanWatch Analytics · Entrega 2** · Tarea 1 del job · Mapa completo del pipeline en el
# MAGIC [README](../README.md#entrega-2--pipeline-en-streaming).
# MAGIC
# MAGIC ## Propósito
# MAGIC
# MAGIC Dejar listo todo lo que el resto del pipeline da por existente: el catálogo con sus esquemas
# MAGIC y Volumes, y las tres dimensiones de referencia que usan plata (la compuerta "en el agua") y
# MAGIC oro (puertos, tipos de buque). Es idempotente de punta a punta: puede correr al inicio de
# MAGIC cada ejecución del job sin efectos secundarios, y si nada cambió en las fuentes no reescribe
# MAGIC nada.
# MAGIC
# MAGIC ## Entradas y salidas
# MAGIC
# MAGIC | | Objeto | Detalle |
# MAGIC |---|---|---|
# MAGIC | **Entrada** | World Port Index (NGA Pub. 150) | CSV, descarga HTTP |
# MAGIC | **Entrada** | Natural Earth 1:10m v5.1.2 | 5 GeoJSON: tierra, lagos (×2), ríos (×2) |
# MAGIC | **Entrada** | `pipeline/seeds/vessel_types.csv` | Semilla versionada en el repo |
# MAGIC | **Entrada** | `pipeline/seeds/world_port_index.csv` | Respaldo del WPI, solo si la NGA no responde y no hay snapshot |
# MAGIC | **Salida** | Catálogo `oceanwatch_lakehouse` (parámetro `catalogo`) | Esquemas `landing`, `bronze`, `silver`, `gold`, `ops` |
# MAGIC | **Salida** | Volumes `landing.ais`, `landing.referencias`, `ops.checkpoints` | Con su estructura de carpetas |
# MAGIC | **Salida** | Permisos de Unity Catalog | Un rol (grupo de la cuenta) por tipo de usuario, por esquema |
# MAGIC | **Salida** | `silver.dim_tipo_buque` | Código AIS → descripción y categoría |
# MAGIC | **Salida** | `silver.dim_puertos` | Puertos del mundo con coordenadas |
# MAGIC | **Salida** | `silver.dim_h3_tierra_firme` | Máscara de celdas H3 de tierra firme |
# MAGIC
# MAGIC **Parámetros:** `catalogo` (por defecto `oceanwatch_lakehouse`).
# MAGIC
# MAGIC ## Contenido
# MAGIC
# MAGIC - **Parte A · Estructura de Unity Catalog**
# MAGIC   - A.1 Catálogo y esquemas
# MAGIC   - A.2 Volumes y carpetas
# MAGIC   - A.3 Permisos por rol
# MAGIC - **Parte B · Fuentes de referencia**
# MAGIC   - B.1 Patrón de snapshot (utilidades)
# MAGIC   - B.2 Contratos de las dimensiones
# MAGIC   - B.3 `dim_tipo_buque`
# MAGIC   - B.4 `dim_puertos`
# MAGIC   - B.5 `dim_h3_tierra_firme` (máscara invertida)
# MAGIC - **Evidencia** · verificación de estructura, puntos de control y resultado de la tarea

# COMMAND ----------

import hashlib
import io
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from pyspark.databricks.sql import functions as dbf  # funciones nativas H3 y espaciales de Databricks
from pyspark.sql import functions as f

import config

# Mismo código en Databricks y en local (Databricks Connect + Files API del SDK)
spark = config.obtener_spark()
w = config.obtener_workspace()

CATALOGO = config.leer_parametro("catalogo", config.CATALOGO_DEFECTO)
APLICAR_PERMISOS = config.leer_parametro("aplicar_permisos", "true").lower() == "true"
cfg = config.Config(CATALOGO)
print(f"Catálogo: {cfg.catalogo}")


def sql_texto(texto: str) -> str:
    """Escapa comillas simples para literales SQL."""
    return texto.replace("'", "\\'")

# COMMAND ----------

# MAGIC %md
# MAGIC # Parte A · Estructura de Unity Catalog
# MAGIC
# MAGIC ## Decisiones
# MAGIC
# MAGIC - **Un esquema por capa.** Los permisos por rol del requisito 5 se otorgan por esquema
# MAGIC   (p. ej. analistas: `SELECT` solo sobre `gold`); con capas mezcladas en un esquema eso no
# MAGIC   sería posible sin otorgar tabla por tabla.
# MAGIC - **Catálogo propio de la Entrega 2.** Aísla este modelo del catálogo `oceanwatch` de la
# MAGIC   Entrega 1; el pipeline no lee ninguna tabla de allá.
# MAGIC - **`CREATE … IF NOT EXISTS` seguido de `COMMENT ON …`.** El primero no actualiza el
# MAGIC   comentario si el objeto ya existía; el segundo sí, sin recrearlo. Así la documentación en
# MAGIC   Unity Catalog siempre coincide con `config.py`.
# MAGIC - **Este notebook no crea las tablas de las capas.** Cada notebook de capa declara las suyas,
# MAGIC   para que el contrato de cada tabla viva junto al código que la escribe.
# MAGIC
# MAGIC ## A.1 Catálogo y esquemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalogo}")
spark.sql(
    f"COMMENT ON CATALOG {cfg.catalogo} IS "
    f"'{sql_texto('OceanWatch Analytics - Entrega 2: lakehouse AIS en streaming (bronce, plata, oro).')}'"
)

for capa, comentario in config.ESQUEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.esquema(capa)}")
    spark.sql(f"COMMENT ON SCHEMA {cfg.esquema(capa)} IS '{sql_texto(comentario)}'")
    print(f"OK esquema {cfg.esquema(capa)}")

# Databricks crea un esquema `default` vacío junto con el catálogo. Se elimina para que la
# estructura muestre solo las capas del diseño. Sin CASCADE: si alguien guardó algo ahí, el DROP
# falla en vez de borrarlo, y solo se avisa.
try:
    spark.sql(f"DROP SCHEMA IF EXISTS {cfg.catalogo}.default")
except Exception as e:
    print(f"AVISO: no se eliminó {cfg.catalogo}.default ({type(e).__name__}); se deja como está.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.2 Volumes y carpetas
# MAGIC
# MAGIC - Las carpetas se crean aquí para que ningún stream falle por una ruta inexistente en la
# MAGIC   primera corrida. Se usa la Files API del SDK en vez de `os.makedirs` porque funciona igual
# MAGIC   en Databricks y en local; crear una carpeta que ya existe no falla.
# MAGIC - **Sin carpeta `_staging/` en el Volume:** los Volumes viven en almacenamiento de objetos,
# MAGIC   donde no existe "renombrar" (un `mv` es copiar + borrar) pero **subir un archivo es
# MAGIC   atómico** (se ve completo o no se ve). El productor arma el archivo en el disco del driver
# MAGIC   y lo sube de una sola vez a `entrantes/`; Auto Loader nunca lee un archivo a medias.

# COMMAND ----------

for (capa, nombre), (comentario, subcarpetas) in config.VOLUMES.items():
    nombre_uc = f"{cfg.esquema(capa)}.{nombre}"
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {nombre_uc}")
    spark.sql(f"COMMENT ON VOLUME {nombre_uc} IS '{sql_texto(comentario)}'")
    for sub in subcarpetas:
        w.files.create_directory(f"{cfg.volume(capa, nombre)}/{sub}")
    print(f"OK volume {nombre_uc}  ->  {subcarpetas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.3 Permisos por rol
# MAGIC
# MAGIC Cada rol accede a la capa que necesita y nada más (roles y privilegios en `config.ROLES`):
# MAGIC
# MAGIC | Rol (grupo) | `landing` | `bronze` | `silver` | `gold` | `ops` |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `ow_ingenieros` | todo | todo | todo | todo | todo |
# MAGIC | `ow_analistas` | — | — | lectura | lectura + modelos | — |
# MAGIC | `ow_consumidores` | — | — | — | lectura | — |
# MAGIC
# MAGIC - **Por esquema, no por tabla:** los privilegios se heredan a las tablas que existen y a las
# MAGIC   que se creen después (oro, modelos de ML), sin volver a tocar los permisos.
# MAGIC - **Los grupos son de la cuenta** (Unity Catalog no reconoce grupos locales del workspace) y
# MAGIC   los crea un administrador; el job no crea identidades. Si un grupo no existe, la tarea
# MAGIC   falla al final con el nombre del grupo: sin roles no hay gobernanza.
# MAGIC - `GRANT` es idempotente: repetirlo no cambia nada. Solo agrega: quitar un privilegio de
# MAGIC   `config.ROLES` no lo revoca (se hace con `REVOKE` a mano, deliberadamente).
# MAGIC
# MAGIC - **Parámetro `aplicar_permisos`** (`true` por defecto). En un workspace sin grupos de cuenta
# MAGIC  (Databricks Free Edition, usado para desarrollo) se pasa `false` y la tarea omite los `GRANT`
# MAGIC  sin fallar. En el workspace institucional siempre se aplican.

# COMMAND ----------

errores_permisos: list[str] = []
if not APLICAR_PERMISOS:
    print("aplicar_permisos=false: se omiten los GRANT (entorno sin grupos de cuenta, p. ej. Free Edition).")
else:
    for grupo, (descripcion, alcance) in config.ROLES.items():
        try:
            if alcance == "catalogo":
                spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {cfg.catalogo} TO `{grupo}`")
            else:
                spark.sql(f"GRANT USE CATALOG ON CATALOG {cfg.catalogo} TO `{grupo}`")
                for capa, privilegios in alcance.items():
                    spark.sql(f"GRANT {', '.join(privilegios)} ON SCHEMA {cfg.esquema(capa)} TO `{grupo}`")
            print(f"OK {grupo}: {descripcion}")
        except Exception as e:
            errores_permisos.append(f"{grupo}: {str(e).splitlines()[0][:200]}")
            print(f"FALLA {grupo}: {errores_permisos[-1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Parte B · Fuentes de referencia
# MAGIC
# MAGIC ## Decisiones
# MAGIC
# MAGIC | Dimensión (silver) | Fuente | Para qué |
# MAGIC |---|---|---|
# MAGIC | `dim_tipo_buque` | Semilla versionada en el repo | Traducir `VesselType` y agrupar por categoría |
# MAGIC | `dim_puertos` | World Port Index (NGA Pub. 150) | Presencia en puertos y ranking (oro) |
# MAGIC | `dim_h3_tierra_firme` | Natural Earth 1:10m v5.1.2 | Compuerta "coordenada en el agua" (plata) |
# MAGIC
# MAGIC **Patrón: snapshot inmutable + reconstrucción por versión.** Las referencias no son
# MAGIC incrementos sino **estados completos** (el catálogo entero de puertos, el mapa entero de
# MAGIC tierra). Por eso, a diferencia del stream AIS, aquí sí es seguro sobrescribir:
# MAGIC
# MAGIC ```
# MAGIC 1. descargar → sha256 del contenido
# MAGIC 2. ¿mismo sha que el último snapshot?  sí → no se escribe archivo nuevo
# MAGIC                                       no → landing/referencias/<fuente>/<timestamp>_<sha>.<ext>
# MAGIC 3. ¿la dimensión ya fue construida con esa versión?  sí → no se hace nada
# MAGIC                                                       no → overwrite (1 commit, atómico)
# MAGIC ```
# MAGIC
# MAGIC - **El archivo en landing es la capa cruda** de la referencia: inmutable (el nombre lleva el
# MAGIC   sha, nunca se sobrescribe con otro contenido) y con historial (un archivo por versión).
# MAGIC - **La versión vive dentro de la dimensión** (columna `_version_fuente`), no en un archivo
# MAGIC   de control aparte: "datos nuevos" y "versión nueva" se escriben en el **mismo commit**. Si
# MAGIC   la tarea muere en cualquier punto, la siguiente corrida compara contra el destino real y
# MAGIC   rehace solo lo que falta. *Se le pregunta al destino, no a un paso intermedio.*
# MAGIC - **Si una fuente no responde:** con snapshot previo, se usa ese y se avisa (una referencia
# MAGIC   de ayer sigue siendo válida hoy); sin ninguno, esa dimensión no se construye y la tarea
# MAGIC   falla **al final**, después de construir las demás (las fuentes son independientes).
# MAGIC
# MAGIC ## B.1 Utilidades de snapshot

# COMMAND ----------

@dataclass(frozen=True)
class Snapshot:
    ruta: str      # ruta completa en el Volume
    sha256: str    # sha256 completo del contenido
    nuevo: bool    # True si se escribió en esta corrida


def descargar(url: str, reintentos: int = 3, timeout: int = 120) -> bytes:
    """Descarga con reintentos y espera creciente. Falla si la respuesta no es 200 o viene vacía."""
    for intento in range(1, reintentos + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            if not r.content:
                raise ValueError("respuesta vacía")
            return r.content
        except (requests.RequestException, ValueError) as e:
            print(f"  intento {intento}/{reintentos} fallido: {e}")
            if intento == reintentos:
                raise
            time.sleep(5 * intento)


def ultimo_snapshot(fuente: str) -> Snapshot | None:
    """Último snapshot de una fuente. Los nombres empiezan por timestamp UTC, así que el orden
    lexicográfico es el orden temporal."""
    carpeta = cfg.ruta_referencia(fuente)
    w.files.create_directory(carpeta)
    archivos = sorted(
        (e for e in w.files.list_directory_contents(carpeta) if not e.is_directory),
        key=lambda e: e.name,
    )
    if not archivos:
        return None
    ultimo = archivos[-1]
    sha = ultimo.name.split("_", 1)[1].split(".", 1)[0]
    return Snapshot(ruta=f"{carpeta}/{ultimo.name}", sha256=sha, nuevo=False)


def registrar_snapshot(fuente: str, contenido: bytes, extension: str) -> Snapshot:
    """Guarda el contenido como snapshot nuevo solo si difiere del último (por sha256)."""
    sha = hashlib.sha256(contenido).hexdigest()
    ultimo = ultimo_snapshot(fuente)
    if ultimo and ultimo.sha256 == sha:
        return ultimo
    marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ruta = f"{cfg.ruta_referencia(fuente)}/{marca}_{sha}.{extension}"
    # overwrite=True es inocuo: el nombre lleva el sha, así que solo podría "sobrescribirse"
    # con el mismo contenido (p. ej. un reintento de esta misma tarea).
    w.files.upload(ruta, io.BytesIO(contenido), overwrite=True)
    return Snapshot(ruta=ruta, sha256=sha, nuevo=True)


def snapshot_desde_url(
    fuente: str, url: str, extension: str, respaldo: str | None = None
) -> Snapshot:
    """Descarga y registra. Si la fuente no responde: 1) último snapshot conocido;
    2) si no hay ninguno, la semilla de respaldo del repo (si existe); 3) si no, error."""
    try:
        snap = registrar_snapshot(fuente, descargar(url), extension)
    except Exception as e:
        previo = ultimo_snapshot(fuente)
        if previo is None and respaldo:
            with open(respaldo, "rb") as fh:
                previo = registrar_snapshot(fuente, fh.read(), extension)
            print(f"AVISO: '{fuente}' no respondió ({e}) y no había snapshots. Se registró la "
                  f"semilla de respaldo {respaldo} como snapshot inicial: {previo.ruta}")
            return previo
        if previo is None:
            raise RuntimeError(
                f"La fuente '{fuente}' no respondió ({e}) y no hay ningún snapshot previo en "
                f"{cfg.ruta_referencia(fuente)}. Sin referencia no se puede validar; reintente "
                f"más tarde o deposite manualmente un snapshot inicial (documentando su origen)."
            ) from e
        print(f"AVISO: '{fuente}' no respondió ({e}). Se usa el último snapshot: {previo.ruta}")
        return previo
    estado = "NUEVO" if snap.nuevo else "sin cambios"
    print(f"{fuente}: {estado} -> {snap.ruta}")
    return snap


def version_construida(tabla: str) -> str | None:
    """Versión de fuente con la que se construyó la dimensión (None si está vacía)."""
    return spark.table(tabla).agg(f.max("_version_fuente").alias("v")).first()["v"]


# Las fuentes son independientes: un error se registra aquí y la tarea falla al final
# (sección "Resultado de la tarea"), después de construir lo que sí se pudo.
errores_fuentes: list[str] = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.2 Contratos de las dimensiones
# MAGIC
# MAGIC Cada tabla se declara con esquema explícito y comentario por columna: es el contrato que
# MAGIC leen plata y oro. `CREATE TABLE IF NOT EXISTS` no altera una tabla existente; el
# MAGIC `overwrite` posterior reemplaza solo los datos y conserva esquema y documentación.

# COMMAND ----------

T_TIPO_BUQUE = cfg.tabla("silver", "dim_tipo_buque")
T_PUERTOS = cfg.tabla("silver", "dim_puertos")
T_TIERRA = cfg.tabla("silver", "dim_h3_tierra_firme")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_TIPO_BUQUE} (
    vessel_type      INT    NOT NULL COMMENT 'Código AIS VesselType (0-99)',
    descripcion      STRING          COMMENT 'Descripción del estándar AIS (NOAA VesselTypeCodes2018)',
    categoria        STRING          COMMENT 'Agrupación de negocio: Carga, Tanquero, Pasajeros, Pesca, Remolque, Recreo, ...',
    _version_fuente  STRING          COMMENT 'sha256 de la semilla con que se construyó la tabla'
) COMMENT 'Dimensión de tipos de buque. Fuente: semilla versionada pipeline/seeds/vessel_types.csv, recreada de NOAA VesselTypeCodes2018.pdf.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_PUERTOS} (
    wpi_id           INT    NOT NULL COMMENT 'World Port Index Number (llave natural)',
    nombre_puerto    STRING          COMMENT 'Main Port Name',
    pais             STRING          COMMENT 'Country Code',
    lat              DOUBLE          COMMENT 'Latitud del puerto (grados decimales)',
    lon              DOUBLE          COMMENT 'Longitud del puerto (grados decimales)',
    tamano_puerto    STRING          COMMENT 'Harbor Size',
    tipo_puerto      STRING          COMMENT 'Harbor Type',
    _version_fuente  STRING          COMMENT 'sha256 del snapshot del WPI con que se construyó la tabla'
) COMMENT 'Dimensión de puertos del mundo. Fuente: World Port Index (NGA Pub. 150), msi.nga.mil. Filas sin coordenadas descartadas.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_TIERRA} (
    h3_celda         BIGINT NOT NULL COMMENT 'Celda H3 (resolución {config.MASCARA_RESOLUCION_H3}) considerada tierra firme',
    _version_fuente  STRING          COMMENT 'Hash de (sha de las 5 capas de Natural Earth + parámetros de la máscara)'
) COMMENT 'Máscara de tierra firme para la compuerta "en el agua": celdas de tierra de Natural Earth 1:10m dentro de la cobertura AIS, menos lagos, menos ríos con margen, menos franja costera. Un punto AIS cuya H3_Micro está aquí va a cuarentena.'
""")
print("Contratos declarados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.3 `dim_tipo_buque` desde la semilla del repo
# MAGIC
# MAGIC La semilla también pasa por el snapshot: queda en landing qué versión exacta del archivo
# MAGIC produjo la tabla, igual que con una fuente externa. Se le agregó la columna `categoria`
# MAGIC (agrupación de negocio) para el tablero.
# MAGIC
# MAGIC **Evidencia (2023-06-01, 8,8 M filas):** el 99,6 % de las filas trae un código 0-99; 33 475
# MAGIC vienen nulas y 765 traen el código 136, que no existe en el estándar. No aparecen los
# MAGIC códigos NAIS 1001-1025, así que el rango 0-99 cubre la fuente; el 136 se tratará en plata
# MAGIC como tipo desconocido (bandera, no cuarentena).

# COMMAND ----------

with open(config.SEMILLA_TIPOS_BUQUE, "rb") as fh:
    snap_tipos = registrar_snapshot("semillas/vessel_types", fh.read(), "csv")
print(f"semilla: {'NUEVO' if snap_tipos.nuevo else 'sin cambios'} -> {snap_tipos.ruta}")

if version_construida(T_TIPO_BUQUE) == snap_tipos.sha256:
    print(f"{T_TIPO_BUQUE} ya está en la versión {snap_tipos.sha256[:12]}: no se reconstruye.")
else:
    df_tipos = (
        spark.read.option("header", True).csv(snap_tipos.ruta)
        .select(
            f.col("vessel_type").try_cast("int").alias("vessel_type"),
            f.col("descripcion"),
            f.col("categoria"),
            f.lit(snap_tipos.sha256).alias("_version_fuente"),
        )
    )
    df_tipos.write.mode("overwrite").saveAsTable(T_TIPO_BUQUE)
    print(f"{T_TIPO_BUQUE} reconstruida: {spark.table(T_TIPO_BUQUE).count()} tipos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.4 `dim_puertos` desde el World Port Index
# MAGIC
# MAGIC Misma selección de columnas que en la Entrega 1. El CSV se lee **sin** `inferSchema`: todo
# MAGIC llega como texto y los tipos se fijan aquí, explícitos.
# MAGIC
# MAGIC **`try_cast` y no `cast`.** Databricks corre en modo ANSI: un `cast` que no puede convertir
# MAGIC un valor **lanza error** y tumba la tarea completa (en la primera corrida falló así:
# MAGIC `CAST_INVALID_INPUT: '7950.0' cannot be cast to INT`, porque la NGA escribe el número de
# MAGIC puerto con decimal). `try_cast` devuelve `NULL` en su lugar; las filas sin llave o sin
# MAGIC coordenadas se descartan y **se cuentan**, así un cambio de formato en la fuente se ve como
# MAGIC descarte medible y no como una caída. El número de puerto pasa por `double` antes de `int`
# MAGIC para aceptar `7950.0`.
# MAGIC
# MAGIC **Semilla de respaldo.** Desde el 2026-11-21 el sitio de la NGA (`msi.nga.mil`) responde
# MAGIC `HTTP 503` (servicio no disponible), tanto en la URL de descarga como en la página pública
# MAGIC del WPI. Para que el pipeline no dependa de esa disponibilidad, el repo incluye
# MAGIC `pipeline/seeds/world_port_index.csv`: el CSV descargado de **la misma URL** el 2025-09-25
# MAGIC durante la Entrega 1 (3 807 puertos, sha256 `315644f1…`). Orden de preferencia:
# MAGIC
# MAGIC 1. Descarga desde la NGA → si el contenido cambió, snapshot nuevo y la dimensión se reconstruye.
# MAGIC 2. Si la NGA no responde → último snapshot en landing.
# MAGIC 3. Si no hay ningún snapshot (primera corrida) → la semilla del repo se registra como
# MAGIC    snapshot inicial. Cuando la NGA vuelva, el paso 1 lo reemplaza solo si el contenido difiere.

# COMMAND ----------

try:
    snap_wpi = snapshot_desde_url(
        "wpi", config.URL_WPI, "csv", respaldo=config.SEMILLA_WPI_RESPALDO
    )
except RuntimeError as e:
    errores_fuentes.append(str(e))
    snap_wpi = None
    print(f"ERROR WPI (se continúa con las demás fuentes): {e}")

if snap_wpi is None:
    pass
elif version_construida(T_PUERTOS) == snap_wpi.sha256:
    print(f"{T_PUERTOS} ya está en la versión {snap_wpi.sha256[:12]}: no se reconstruye.")
else:
    df_wpi = (
        spark.read.option("header", True).csv(snap_wpi.ruta)
        .select(
            f.col("World Port Index Number").try_cast("double").try_cast("int").alias("wpi_id"),
            f.col("Main Port Name").alias("nombre_puerto"),
            f.col("Country Code").alias("pais"),
            f.col("Latitude").try_cast("double").alias("lat"),
            f.col("Longitude").try_cast("double").alias("lon"),
            f.col("Harbor Size").alias("tamano_puerto"),
            f.col("Harbor Type").alias("tipo_puerto"),
            f.lit(snap_wpi.sha256).alias("_version_fuente"),
        )
    )
    valido = f.col("wpi_id").isNotNull() & f.col("lat").isNotNull() & f.col("lon").isNotNull()
    conteo = df_wpi.agg(
        f.count("*").alias("leidas"), f.count(f.when(~valido, 1)).alias("descartadas")
    ).first()
    print(f"WPI: {conteo.leidas:,} filas leídas, {conteo.descartadas:,} descartadas sin llave o coordenadas")

    df_puertos = df_wpi.filter(valido)
    df_puertos.write.mode("overwrite").saveAsTable(T_PUERTOS)
    print(f"{T_PUERTOS} reconstruida: {spark.table(T_PUERTOS).count():,} puertos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.5 `dim_h3_tierra_firme`: la máscara invertida
# MAGIC
# MAGIC Se guarda la **tierra**, no el agua: a resolución 7 (~5 km² por celda) el océano son decenas
# MAGIC de millones de celdas, mientras que la tierra dentro de la cobertura AIS son ~4 M.
# MAGIC
# MAGIC ```
# MAGIC candidata    = tierra (polyfill)            celdas cuyo centro cae en tierra
# MAGIC              − lagos (cover)                celdas que tocan un lago
# MAGIC              − ríos  (cover + k_rios)       celdas que toca la línea central del río, más k anillos
# MAGIC tierra_firme = candidata
# MAGIC              − costa (k_costa)              celdas candidatas con alguna vecina (a k anillos) que no es candidata
# MAGIC              − puertos (k_puertos)          celdas a k anillos o menos de un puerto del WPI
# MAGIC               − transitadas                  celdas con 5+ MMSI distintos (semilla calibrada)
# MAGIC ```
# MAGIC
# MAGIC - **Por qué restar ríos:** el AIS de EE. UU. incluye tráfico fluvial real (remolcadores y
# MAGIC   barcazas en el Mississippi, el Ohio, etc.). En un polígono de tierra a 1:10m un río de 1 km
# MAGIC   de ancho *es* tierra; sin esta resta, tráfico legítimo iría a cuarentena.
# MAGIC - **Por qué restar la costa y los puertos (calibración con datos):** con la primera versión
# MAGIC   (`k_costa = 1`, sin zona portuaria) el **11,4 %** de las posiciones del 2023-06-01 caía
# MAGIC   "en tierra" (1 007 548 filas, 3 382 buques). El diagnóstico mostró que no eran errores de
# MAGIC   GPS sino **puertos**: el 81 % estaban detenidos, y las zonas principales eran el canal de
# MAGIC   Houston (252 870 filas, 428 buques), los pantanos de Luisiana, Nueva Orleans, Port
# MAGIC   Everglades, Norfolk y Seattle. La escala 1:10m no dibuja canales de navegación, dársenas
# MAGIC   ni marinas. Se midieron 16 combinaciones:
# MAGIC
# MAGIC   | Franja costera | Zona portuaria | Filas "en tierra" | % | Buques |
# MAGIC   |---|---|---|---|---|
# MAGIC   | k=1 (inicial) | — | 1 007 548 | 11,44 % | 3 382 |
# MAGIC   | k=3 | — | 238 052 | 2,70 % | 872 |
# MAGIC   | k=1 | k=8 (~10 km) | 265 812 | 3,02 % | 1 159 |
# MAGIC   | **k=3** | **k=8 (~10 km)** | **119 514** | **1,36 %** | **493** |
# MAGIC   | k=4 | k=12 (~14 km) | 87 331 | 0,99 % | 306 |
# MAGIC
# MAGIC   Se eligió `k_costa = 3`, `k_puertos = 8`: más allá, cada ampliación quita poco y vuelve la
# MAGIC   máscara permisiva con errores reales cerca de la costa. El ~1,4 % restante va a cuarentena
# MAGIC   y se documenta como **limitación conocida** (incluye algo de tráfico real en canales que
# MAGIC   la escala 1:10m no resuelve); no se pierde, queda en la cuarentena con su motivo. No se
# MAGIC   usó una costa más precisa (OSM, NOAA, USGS) por costo de descarga y procesamiento.
# MAGIC
# MAGIC   - **Por qué restar las celdas transitadas:** con costa k=3 y puertos k=8 quedaba un 1,25 % de
# MAGIC   posiciones "en tierra", concentrado en canales y dársenas reales. Una celda por la que pasan 5
# MAGIC   o más buques distintos no es tierra firme. Se calibró con 12 días (2023-06-01 a 06-12): 222
# MAGIC   celdas, con mediana de 82 buques y máximo de 338. Se guarda como semilla versionada, y no se
# MAGIC   recalcula en cada corrida, para que la máscara sea idéntica en cualquier workspace y no cambie
# MAGIC   al entrar datos nuevos (la conciliación global de plata depende de eso).
# MAGIC
# MAGIC   **Resultado tras reconstruir** (la franja de 3 anillos se mide contra cualquier agua: mar,
# MAGIC   lago o río): máscara de 2 099 026 celdas (antes 3 207 452) y **109 897 filas "en tierra"
# MAGIC   (1,25 %), 484 buques** en el 2023-06-01.
# MAGIC - **Por qué la versión incluye los parámetros:** cambiar un `k` o una región al calibrar
# MAGIC   cambia la versión y fuerza la reconstrucción, sin intervención manual.
# MAGIC - **Por qué en mosaicos de 5°:** cada capa de Natural Earth trae pocos multipolígonos enormes
# MAGIC   (`ne_10m_land` tiene 11 features: continentes enteros) y Spark procesa una fila en una sola
# MAGIC   tarea. Cortar cada feature en mosaicos de 5°×5° convierte un polígono continental en
# MAGIC   cientos de filas pequeñas que se rellenan en paralelo. Las celdas no se duplican en los
# MAGIC   bordes porque `h3_polyfillash3` asigna cada celda según su centro, y además se toma
# MAGIC   `DISTINCT`.
# MAGIC
# MAGIC
# MAGIC **Evidencia** (capa de tierra, mismas 4 regiones, serverless, Databricks Connect):
# MAGIC
# MAGIC | Variante | Tiempo | Celdas (4 regiones) |
# MAGIC |---|---|---|
# MAGIC | Feature completo recortado a la región (1 fila por continente) | 546,7 s | 3 945 572 |
# MAGIC | Mosaicos de 5°, `repartition(256)` **después** del recorte y relleno (pista SQL en el mismo `SELECT`) | 24,9 s / 23,8 s | 3 945 572 |
# MAGIC | Mosaicos de 5°, `repartition(256)` **antes** del recorte y relleno (esta implementación) | 11,1 s / 10,2 s | 3 945 572 |
# MAGIC
# MAGIC Las tres variantes producen exactamente las mismas celdas; solo cambia cuánto trabajo cae
# MAGIC en paralelo. Las dos últimas filas se midieron intercaladas en la misma sesión (dos rondas)
# MAGIC para que la comparación sea justa. La diferencia entre ellas no es PySpark frente a SQL
# MAGIC (ambos generan planes en el mismo motor) sino **dónde queda el reparto**: repartir después
# MAGIC del cálculo caro no paraleliza el cálculo caro.

# COMMAND ----------

TAMANO_MOSAICO_GRADOS = 5.0
RES = config.MASCARA_RESOLUCION_H3

# Snapshots de las 5 capas; si alguna falla sin snapshot previo, la máscara no se construye.
snaps_ne: dict[str, Snapshot] = {}
for capa, (_, url) in config.CAPAS_NATURAL_EARTH.items():
    try:
        snaps_ne[capa] = snapshot_desde_url(f"natural_earth/{capa}", url, "geojson")
    except RuntimeError as e:
        errores_fuentes.append(str(e))
        print(f"ERROR Natural Earth {capa} (la máscara no se construye en esta corrida): {e}")

# Celdas transitadas: semilla versionada (99_utilidades, "Calibración de la máscara").
with open(config.SEMILLA_CELDAS_TRANSITADAS, "rb") as fh:
    snap_transitadas = registrar_snapshot("semillas/h3_celdas_transitadas", fh.read(), "csv")
print(f"semilla celdas transitadas: {'NUEVO' if snap_transitadas.nuevo else 'sin cambios'} -> {snap_transitadas.ruta}")

# La máscara usa los puertos (zona portuaria): sin WPI construido en esta corrida no se reconstruye.
MASCARA_DISPONIBLE = len(snaps_ne) == len(config.CAPAS_NATURAL_EARTH) and snap_wpi is not None

if MASCARA_DISPONIBLE:
    parametros_mascara = {
        "capas": {capa: s.sha256 for capa, s in sorted(snaps_ne.items())},
        "wpi": snap_wpi.sha256,
        "regiones": config.REGIONES_AIS,
        "resolucion": RES,
        "k_rios": config.MASCARA_K_RIOS,
        "k_costa": config.MASCARA_K_COSTA,
        "k_puertos": config.MASCARA_K_PUERTOS,
        "transitadas": snap_transitadas.sha256,
        "min_buques": config.MASCARA_MIN_BUQUES,
        "mosaico": TAMANO_MOSAICO_GRADOS,
    }
    VERSION_MASCARA = hashlib.sha256(
        json.dumps(parametros_mascara, sort_keys=True).encode()
    ).hexdigest()
    RECONSTRUIR_MASCARA = version_construida(T_TIERRA) != VERSION_MASCARA
    print(f"Versión de la máscara: {VERSION_MASCARA[:12]} · reconstruir: {RECONSTRUIR_MASCARA}")
else:
    RECONSTRUIR_MASCARA = False

# COMMAND ----------

def leer_geometrias(capa: str, rol: str, ruta: str):
    """Lee un GeoJSON (FeatureCollection) como filas (capa, rol, nombre, geojson).

    `geometry` se declara STRING: con ese tipo, from_json conserva el objeto JSON original como
    texto, que es justo lo que recibe st_geomfromgeojson.
    """
    esquema = "features ARRAY<STRUCT<properties: MAP<STRING, STRING>, geometry: STRING>>"
    return (
        spark.read.text(ruta, wholetext=True)
        .select(f.from_json("value", esquema).alias("fc"))
        .select(f.explode("fc.features").alias("ft"))
        .select(
            f.lit(capa).alias("capa"),
            f.lit(rol).alias("rol"),
            f.col("ft.properties")["name"].alias("nombre"),
            f.col("ft.geometry").alias("geojson"),
        )
        .filter(f.col("geojson").isNotNull())
    )


def mosaicos(regiones: dict, paso: float) -> list[tuple[str, str]]:
    """Corta cada caja (lon_min, lat_min, lon_max, lat_max) en mosaicos de `paso` grados (WKT)."""
    salida = []
    for region, (x0, y0, x1, y1) in regiones.items():
        x = x0
        while x < x1:
            xa = min(x + paso, x1)
            y = y0
            while y < y1:
                ya = min(y + paso, y1)
                salida.append((region, f"POLYGON(({x} {y},{xa} {y},{xa} {ya},{x} {ya},{x} {y}))"))
                y = ya
            x = xa
    return salida


if RECONSTRUIR_MASCARA:
    geometrias = None
    for capa, (rol, _) in config.CAPAS_NATURAL_EARTH.items():
        df = leer_geometrias(capa, rol, snaps_ne[capa].ruta)
        geometrias = df if geometrias is None else geometrias.unionByName(df)

    lista_mosaicos = mosaicos(config.REGIONES_AIS, TAMANO_MOSAICO_GRADOS)
    df_mosaicos = spark.createDataFrame(lista_mosaicos, "region STRING, wkt STRING")
    print(f"{len(lista_mosaicos)} mosaicos de {TAMANO_MOSAICO_GRADOS}° sobre {len(config.REGIONES_AIS)} regiones")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Celdas por rol
# MAGIC
# MAGIC Se recorta cada geometría a cada mosaico que toca y se convierte en celdas: `polyfill`
# MAGIC (centro adentro) para la tierra y `cover` (cualquier contacto) para lagos y ríos, porque para
# MAGIC *restar* agua conviene ser generoso. Las variantes `h3_try_*` devuelven `NULL` en vez de
# MAGIC abortar si un recorte no es convertible; esas fallas se cuentan y se reportan.
# MAGIC
# MAGIC Las funciones H3 y espaciales se usan desde `pyspark.databricks.sql.functions` (`dbf`):
# MAGIC son las funciones nativas de Databricks expuestas en la API de DataFrame, sin SQL embebido
# MAGIC en strings (la Entrega 1 usaba `f.expr("h3_...")` para lo mismo).
# MAGIC
# MAGIC El `repartition(256)` va **después** del join y **antes** del recorte y el relleno: es ahí
# MAGIC donde está el costo, y así se reparte en 256 tareas en vez de unas pocas.

# COMMAND ----------

if RECONSTRUIR_MASCARA:
    t_inicio_mascara = time.time()
    recortes = (
        geometrias.withColumn("g", dbf.st_geomfromgeojson("geojson"))
        .join(
            df_mosaicos.withColumn("m", dbf.st_geomfromtext("wkt", 4326)),
            on=dbf.st_intersects("g", "m"),
        )
        .repartition(256)
        .withColumn("recorte", dbf.st_asbinary(dbf.st_intersection("g", "m")))
        .withColumn(
            "celdas",
            f.when(f.col("rol") == "tierra", dbf.h3_try_polyfillash3("recorte", RES))
            .otherwise(dbf.h3_try_coverash3("recorte", RES)),
        )
        .select("capa", "rol", "nombre", "region", "celdas")
    )

    (
        recortes.groupBy("capa", "rol")
        .agg(
            f.count("*").alias("recortes"),
            f.count(f.when(f.col("celdas").isNull(), 1)).alias("recortes_no_convertibles"),
            f.sum(f.size("celdas")).alias("celdas"),
        )
        .orderBy("rol", "capa")
        .show(truncate=False)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Álgebra de conjuntos y escritura
# MAGIC
# MAGIC Cada resta es un `left_anti` join: "quédate con las celdas de la izquierda que **no**
# MAGIC aparecen a la derecha". La franja costera se obtiene sin geometría: una celda candidata es
# MAGIC costera si alguna de sus vecinas a `k_costa` anillos (`h3_kring`) no es candidata (es mar,
# MAGIC lago o río). La zona portuaria son los `k_puertos` anillos alrededor de la celda de cada
# MAGIC puerto de `dim_puertos`.

# COMMAND ----------

if RECONSTRUIR_MASCARA:
    celdas_por_rol = recortes.select("rol", f.explode("celdas").alias("celda"))

    tierra = celdas_por_rol.filter(f.col("rol") == "tierra").select("celda").distinct()
    lagos = celdas_por_rol.filter(f.col("rol") == "lago").select("celda").distinct()
    rios = (
        celdas_por_rol.filter(f.col("rol") == "rio")
        .select(f.explode(dbf.h3_kring("celda", config.MASCARA_K_RIOS)).alias("celda"))
        .distinct()
    )
    candidata = (
        tierra
        .join(lagos, on="celda", how="left_anti")
        .join(rios, on="celda", how="left_anti")
    )
    costa = (
        candidata.select("celda", f.explode(dbf.h3_kring("celda", config.MASCARA_K_COSTA)).alias("vecino"))
        .join(candidata.withColumnRenamed("celda", "vecino"), on="vecino", how="left_anti")
        .select("celda")
        .distinct()
    )
    zona_portuaria = (
        spark.table(T_PUERTOS)
        .select(dbf.h3_longlatash3("lon", "lat", RES).alias("centro"))
        .select(f.explode(dbf.h3_kring("centro", config.MASCARA_K_PUERTOS)).alias("celda"))
        .distinct()
    )

    transitadas = (
        spark.read.option("header", True).csv(snap_transitadas.ruta)
        .select(f.col("h3_celda").cast("bigint").alias("celda"))
    )
    df_tierra_firme = (
        candidata
        .join(costa, on="celda", how="left_anti")
        .join(zona_portuaria, on="celda", how="left_anti")
        .join(transitadas, on="celda", how="left_anti")
        .select(f.col("celda").alias("h3_celda"), f.lit(VERSION_MASCARA).alias("_version_fuente"))
    )
    df_tierra_firme.write.mode("overwrite").saveAsTable(T_TIERRA)
    print(
        f"{T_TIERRA} reconstruida: {spark.table(T_TIERRA).count():,} celdas de tierra firme "
        f"en {time.time() - t_inicio_mascara:,.1f} s."
    )
elif MASCARA_DISPONIBLE:
    print(f"{T_TIERRA} ya está en la versión {VERSION_MASCARA[:12]}: no se reconstruye.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Evidencia
# MAGIC
# MAGIC ## Estructura creada

# COMMAND ----------

spark.sql(f"SHOW SCHEMAS IN {cfg.catalogo}").show(truncate=False)

for (capa, nombre), (_, subcarpetas) in config.VOLUMES.items():
    ruta = cfg.volume(capa, nombre)
    existentes = sorted(e.name for e in w.files.list_directory_contents(ruta))
    faltantes = set(subcarpetas) - set(existentes)
    assert not faltantes, f"Faltan carpetas en {ruta}: {faltantes}"
    print(f"{ruta}: {existentes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permisos efectivos por rol

# COMMAND ----------

roles = list(config.ROLES)
permisos = spark.sql(f"SHOW GRANTS ON CATALOG {cfg.catalogo}")
for capa in config.ESQUEMAS:
    permisos = permisos.unionByName(spark.sql(f"SHOW GRANTS ON SCHEMA {cfg.esquema(capa)}"))
(
    permisos.filter(f.col("Principal").isin(roles))
    .groupBy("Principal", "ObjectType", "ObjectKey")
    .agg(f.concat_ws(", ", f.sort_array(f.collect_set("ActionType"))).alias("privilegios"))
    .orderBy("Principal", "ObjectType", "ObjectKey")
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Puntos de control de la máscara
# MAGIC
# MAGIC Pruebas de humo. Las dos primeras son **asserts** (si fallan, la máscara está mal y la
# MAGIC tarea debe fallar). Las de ríos se **reportan**: dependen de qué tan bien Natural Earth cubre
# MAGIC cada río, y su ajuste es lo que se calibra con datos reales. Los puntos fluviales y el del
# MAGIC canal de Houston son posiciones reales del archivo AIS del 2023-06-01 (el de Houston es el
# MAGIC centro de la zona que más posiciones aportaba "en tierra" antes de la calibración).

# COMMAND ----------

if MASCARA_DISPONIBLE:
    puntos_prueba = [
        # (nombre, lon, lat, debe_ser_tierra_firme, es_assert)
        ("Golfo de México (mar abierto)", -90.0, 26.0, False, True),
        ("Kansas (tierra adentro)", -98.5, 38.5, True, True),
        ("Río Mississippi - ferry ST FRANCISVILLE", -91.22283, 30.28335, False, False),
        ("Río Ohio, Pittsburgh - GALE R RHODES", -80.07596, 40.49818, False, False),
        ("Canal de navegación de Houston", -95.11, 29.74, False, False),
    ]
    resultado = (
        spark.createDataFrame(
            puntos_prueba, "nombre STRING, lon DOUBLE, lat DOUBLE, esperado BOOLEAN, es_assert BOOLEAN"
        )
        .withColumn("h3_celda", dbf.h3_longlatash3("lon", "lat", RES))
        .join(spark.table(T_TIERRA).select("h3_celda", f.lit(True).alias("en_tierra")),
              on="h3_celda", how="left")
        .select(
            "nombre",
            f.col("esperado").alias("esperado_tierra_firme"),
            f.coalesce("en_tierra", f.lit(False)).alias("en_tierra_firme"),
            "es_assert",
        )
        .collect()
    )

    for r in resultado:
        ok = r.esperado_tierra_firme == r.en_tierra_firme
        marca = "OK " if ok else ("FALLA" if r.es_assert else "REVISAR")
        print(f"[{marca}] {r.nombre}: tierra_firme={r.en_tierra_firme} (esperado {r.esperado_tierra_firme})")
        if r.es_assert:
            assert ok, f"La máscara clasifica mal un punto de control: {r.nombre}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado de la tarea
# MAGIC
# MAGIC Si alguna fuente no pudo construirse (y no había snapshot previo) o falta algún grupo de los
# MAGIC roles, la tarea falla aquí: el job la marca como fallida y aplica sus reintentos, pero lo que
# MAGIC sí se pudo actualizar ya quedó escrito.

# COMMAND ----------

errores = [f"Fuente: {e}" for e in errores_fuentes] + [f"Permisos: {e}" for e in errores_permisos]
if errores:
    raise RuntimeError("Preparación incompleta:\n- " + "\n- ".join(errores))
print("Estructura, permisos y dimensiones de referencia al día.")

# COMMAND ----------


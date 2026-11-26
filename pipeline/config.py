"""Configuración compartida del pipeline OceanWatch (Entrega 2).

Fuente única de nombres: catálogo, esquemas, Volumes, rutas y tablas. Ningún notebook escribe
un nombre de tabla o una ruta "a mano"; todos la toman de aquí. Así, mover el pipeline a otro
workspace (p. ej. el institucional) es cambiar el parámetro `catalogo`, no el código.

Uso desde un notebook del mismo directorio:

    import config
    catalogo = config.leer_parametro("catalogo", config.CATALOGO_DEFECTO)
    cfg = config.Config(catalogo)
    cfg.tabla("silver", "ais_posiciones")   # -> oceanwatch_lakehouse.silver.ais_posiciones
    cfg.ruta_ais_entrantes                  # -> /Volumes/oceanwatch_lakehouse/landing/ais/entrantes
"""

import os
from dataclasses import dataclass

# ─── Valores por defecto (sobrescribibles con parámetros del job / widgets) ───
CATALOGO_DEFECTO = "oceanwatch_lakehouse"
PERFIL_LOCAL = "uniandes"  # perfil de ~/.databrickscfg para Databricks Connect en local

# ─── Esquemas: uno por capa, más `landing` (archivos) y `ops` (operación) ───
# El comentario de cada esquema queda en Unity Catalog (gobernanza: documentación del contrato).
ESQUEMAS = {
    "landing": "Zona de aterrizaje: Volumes con los archivos tal como llegan de las fuentes "
               "(AIS de NOAA, World Port Index, Natural Earth, semillas). Sin tablas.",
    "bronze": "Capa bronce: crudo inmutable, solo append. Todas las columnas de la fuente como "
              "string + columnas técnicas de linaje (archivo de origen, hora de ingesta).",
    "silver": "Capa plata: registros validados por las compuertas de calidad, deduplicados por "
              "(MMSI, H3_Macro, BaseDateTime); cuarentena con causa; métricas de calidad; "
              "dimensiones de referencia.",
    "gold": "Capa oro: modelo del negocio recalculado desde plata (trayectorias diarias, "
            "presencia en puertos, tráfico por celda y hora). Solo contiene registros que "
            "pasaron las compuertas.",
    "ops": "Operación del pipeline: checkpoints de Structured Streaming, esquemas de Auto Loader "
           "y log de ejecuciones (monitoreo).",
}

# ─── Volumes: (esquema, volume) -> (comentario, subcarpetas a crear) ───
VOLUMES = {
    ("landing", "ais"): (
        "Archivos AIS. _descargas/: zip originales de NOAA (copia cruda y caché para no volver a "
        "descargar). entrantes/: un CSV completo por día, el que consume el stream de bronce. "
        "Cada archivo se sube de una sola vez (subida atómica del almacenamiento de objetos: "
        "Auto Loader nunca ve un archivo a medias) y con nombre inmutable (nunca se sobrescribe).",
        ["_descargas", "entrantes"],
    ),
    ("landing", "referencias"): (
        "Snapshots de fuentes de referencia, una carpeta por fuente y un archivo por fecha de "
        "descarga: wpi/ (World Port Index), natural_earth/ (tierra, lagos, ríos), semillas/ "
        "(catálogos versionados en el repo, p. ej. tipos de buque).",
        ["wpi", "natural_earth", "semillas"],
    ),
    ("ops", "checkpoints"): (
        "Estado de Structured Streaming: un checkpoint por query (bronze, silver, gold) y los "
        "esquemas inferidos/declarados de Auto Loader. Borrar un checkpoint obliga a reprocesar "
        "desde cero; el pipeline está diseñado para que eso sea idempotente.",
        ["bronze", "silver", "gold", "autoloader_schemas"],
    ),
}


# ─── Productor del feed AIS (01_productor.py) ───
URL_BASE_NOAA = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"

# Reloj simulado: si el job no recibe `fecha`, entrega el día de datos que corresponde a "hoy":
#   fecha_datos = DIA_CERO_DATOS + (hoy UTC − INICIO_SIMULACION)
# Así cada día real entrega un día distinto de 2023, y correr el job dos veces el mismo día
# entrega el mismo archivo (que ya existe → no se hace nada).
DIA_CERO_DATOS = "2023-06-08"      # primer día del feed (los días 1-7 entran como backfill)
INICIO_SIMULACION = "2026-11-25"   # día real en que arrancó la simulación


# ─── Fuentes de referencia (00_preparacion.py, parte B) ───
URL_WPI = (
    "https://msi.nga.mil/api/publications/download"
    "?type=view&key=16920959/SFH00000/UpdatedPub150.csv"
)

# Natural Earth 1:10m, versión fijada (tag v5.1.2) para que la máscara sea reproducible:
# la misma versión siempre produce las mismas celdas.
_NE_BASE = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/v5.1.2/geojson"
CAPAS_NATURAL_EARTH = {
    # capa: (rol en la máscara, url)
    "ne_10m_land": ("tierra", f"{_NE_BASE}/ne_10m_land.geojson"),
    "ne_10m_lakes": ("lago", f"{_NE_BASE}/ne_10m_lakes.geojson"),
    "ne_10m_lakes_north_america": ("lago", f"{_NE_BASE}/ne_10m_lakes_north_america.geojson"),
    "ne_10m_rivers_lake_centerlines": ("rio", f"{_NE_BASE}/ne_10m_rivers_lake_centerlines.geojson"),
    "ne_10m_rivers_north_america": ("rio", f"{_NE_BASE}/ne_10m_rivers_north_america.geojson"),
}

SEMILLA_TIPOS_BUQUE = "seeds/vessel_types.csv"  # relativa a pipeline/

# Respaldo del WPI: copia del CSV descargado de URL_WPI el 2025-09-25 (Entrega 1). Solo se usa si
# la NGA no responde y aún no existe ningún snapshot del WPI en landing.
SEMILLA_WPI_RESPALDO = "seeds/world_port_index.csv"

# ─── Máscara de tierra firme (regla "en el agua" de silver) ───
# Cobertura del AIS de MarineCadastre (aguas de EE. UU.). Solo se calcula tierra dentro de estas
# cajas: fuera de ellas no hay tráfico que validar y el costo de rellenar continentes enteros con
# celdas H3 sería innecesario. (lon_min, lat_min, lon_max, lat_max)
REGIONES_AIS = {
    "norteamerica_caribe_hawai_alaska": (-180.0, 15.0, -60.0, 75.0),
    "aleutianas_oeste": (170.0, 50.0, 180.0, 56.0),
    "guam_marianas": (144.0, 13.0, 146.5, 21.0),
    "samoa_americana": (-171.5, -15.0, -168.5, -13.0),
}
MASCARA_RESOLUCION_H3 = 7  # = H3_Micro de silver (~5 km² por celda)
MASCARA_K_RIOS = 1         # anillos de celdas vecinas que se le dan de margen a cada río
# Calibrados con el día 2023-06-01 (ver BITACORA.md): con k_costa=1 y sin zona portuaria, el
# 11,4 % de las posiciones caía "en tierra" (puertos y canales que la escala 1:10m no dibuja);
# con k_costa=3 y k_puertos=8 baja a 1,25 % (109 897 filas, 484 buques).
MASCARA_K_COSTA = 3        # franja tolerada junto a cualquier agua (~3,5 km): muelles, dársenas, costa gruesa
MASCARA_K_PUERTOS = 8      # radio alrededor de cada puerto del WPI considerado navegable (~10 km)

# ─── Oro (04_gold.py) ───
# Trayectorias: reglas de corte de viaje de la Entrega 1
VIAJE_MAX_SALTO_H3 = 2        # salto de más de 2 celdas H3 res 7 entre reportes consecutivos
VIAJE_MAX_SILENCIO_H = 12     # más de 12 h sin reportar
VELOCIDAD_MAX_KN = 60         # velocidad implícita físicamente imposible para un buque
# Tráfico por celda y hora: H3 res 5 (~250 km²). Medido el 2023-06-01: 10 288 celdas, 70 876
# filas celda×hora (res 7 serían 371 365); se ve bien en un mapa nacional.
RES_TRAFICO = 5
# Presencia en puertos: área = anillo de 5 celdas H3 res 8 (~4 km) alrededor del punto del WPI
# (como en la Entrega 1). "En puerto" = en el área y (Status fondeado/amarrado o SOG < 1 nudo).
# Medido el 2023-06-01 frente a la regla de la Entrega 1 (Status 1/5 o nulo): falsos positivos
# navegando a ≥ 5 nudos 77 384 → 14 701; suma 1,3 M posiciones de buques detenidos con otro Status.
PUERTO_RES_H3 = 8
PUERTO_K_ANILLOS = 5
EN_PUERTO_STATUS = (1, 5)
EN_PUERTO_SOG_MAX = 1.0


def en_databricks() -> bool:
    """True si el código corre dentro de Databricks (notebook o job); False en local."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def obtener_spark():
    """Sesión de Spark que funciona igual en Databricks y en local.

    - En Databricks: la sesión del cluster / serverless que ya existe.
    - En local: Databricks Connect contra serverless, con el perfil PERFIL_LOCAL. El código se
      escribe en local pero el cómputo corre en el workspace.
    """
    if en_databricks():
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.profile(PERFIL_LOCAL).serverless(True).getOrCreate()


def obtener_workspace():
    """Cliente del SDK de Databricks para operaciones de archivos en Volumes (Files API).

    Se usa en vez de `os` / `dbutils.fs` porque funciona igual en Databricks y en local: en local
    `/Volumes/...` no existe como ruta del disco, pero la Files API sí la alcanza por red.
    """
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient() if en_databricks() else WorkspaceClient(profile=PERFIL_LOCAL)


def leer_parametro(nombre: str, defecto: str) -> str:
    """Lee un parámetro del job/widget; si no existe (p. ej. en local), usa el valor por defecto.

    Crea el widget si no existe, para que el notebook también muestre el parámetro al correrlo
    a mano. En un job, el valor del parámetro de la tarea sobrescribe al del widget.

    En local no hay widgets: se lee la variable de entorno OW_<NOMBRE> (p. ej. OW_FECHA).
    """
    if not en_databricks():
        return os.environ.get(f"OW_{nombre.upper()}", defecto)
    try:
        from databricks.sdk.runtime import dbutils

        dbutils.widgets.text(nombre, defecto)
        valor = dbutils.widgets.get(nombre)
        return valor if valor else defecto
    except Exception:
        return defecto


@dataclass(frozen=True)
class Config:
    """Nombres y rutas derivados del catálogo. Inmutable: se construye una vez por notebook."""

    catalogo: str = CATALOGO_DEFECTO

    # ── nombres de Unity Catalog ──
    def esquema(self, capa: str) -> str:
        if capa not in ESQUEMAS:
            raise ValueError(f"Esquema desconocido: {capa}. Válidos: {list(ESQUEMAS)}")
        return f"{self.catalogo}.{capa}"

    def tabla(self, capa: str, nombre: str) -> str:
        return f"{self.esquema(capa)}.{nombre}"

    def volume(self, capa: str, nombre: str) -> str:
        return f"/Volumes/{self.catalogo}/{capa}/{nombre}"

    # ── rutas de archivos ──
    @property
    def ruta_ais_descargas(self) -> str:
        return f"{self.volume('landing', 'ais')}/_descargas"

    @property
    def ruta_ais_entrantes(self) -> str:
        return f"{self.volume('landing', 'ais')}/entrantes"

    def ruta_referencia(self, fuente: str) -> str:
        return f"{self.volume('landing', 'referencias')}/{fuente}"

    def ruta_checkpoint(self, query: str) -> str:
        return f"{self.volume('ops', 'checkpoints')}/{query}"

    def ruta_esquema_autoloader(self, query: str) -> str:
        return f"{self.volume('ops', 'checkpoints')}/autoloader_schemas/{query}"

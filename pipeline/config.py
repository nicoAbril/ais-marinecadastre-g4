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
    "ops": "Operación del pipeline: checkpoints de Structured Streaming y esquemas de Auto Loader.",
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
        "Estado de Structured Streaming: un checkpoint por query (bronze, silver) y los "
        "esquemas inferidos/declarados de Auto Loader. Borrar un checkpoint obliga a reprocesar "
        "desde cero; el pipeline está diseñado para que eso sea idempotente.",
        ["bronze", "silver", "autoloader_schemas"],
    ),
}


# ─── Gobernanza: roles (grupos de la cuenta) y permisos (00_preparacion.py, A.3) ───
# Permisos por esquema, no por tabla: en Unity Catalog se heredan a las tablas actuales y
# futuras. Los grupos deben existir en la cuenta (los crea un administrador; el job no crea
# identidades). Al migrar de workspace, se cambian los nombres de los grupos aquí.
# Valor por rol: "catalogo" = todos los privilegios sobre el catálogo; o {esquema: privilegios}.
ROLES = {
    "ow_ingenieros": ("Equipo de datos que opera el pipeline", "catalogo"),
    "ow_analistas": ("Tablero, ML y análisis de calidad: leen plata y oro",
                     {"silver": ["USE SCHEMA", "SELECT"], "gold": ["USE SCHEMA", "SELECT", "EXECUTE"]}),
    "ow_consumidores": ("Autoridades portuarias y navieras: solo el modelo del negocio",
                        {"gold": ["USE SCHEMA", "SELECT"]}),
}

# ─── Productor del feed AIS (01_productor.py) ───
URL_BASE_NOAA = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"

# Reloj simulado: el parámetro `fecha` del job es la fecha del día (real) y se traduce a un día
# de datos:  fecha_datos = DIA_CERO_DATOS + (fecha − INICIO_SIMULACION)
# Así cada día real entrega un día distinto de 2023, y correr el job dos veces con la misma fecha
# entrega el mismo archivo (que ya existe → no se hace nada). Ej.: 2026-11-19 → 2023-06-02.
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
# Calibrados con el día 2023-06-01 (tabla completa en 00_preparacion): con k_costa=1 y sin zona portuaria, el
# 11,4 % de las posiciones caía "en tierra" (puertos y canales que la escala 1:10m no dibuja);
# con k_costa=3 y k_puertos=8 baja a 1,25 % (109 897 filas, 484 buques).
MASCARA_K_COSTA = 3        # franja tolerada junto a cualquier agua (~3,5 km): muelles, dársenas, costa gruesa
MASCARA_K_PUERTOS = 8      # radio alrededor de cada puerto del WPI considerado navegable (~10 km)


# Celdas transitadas: una celda H3 res 7 por la que pasan MASCARA_MIN_BUQUES o más MMSI distintos no
# es tierra firme (canales, dársenas y ríos que Natural Earth 1:10m no dibuja). Se calibra una vez
# y se congela como semilla versionada, para que la máscara sea reproducible en cualquier workspace
# y no cambie sola al entrar datos (cálculo documentado en 00_preparacion, sección B.5).
MASCARA_MIN_BUQUES = 5
SEMILLA_CELDAS_TRANSITADAS = "seeds/h3_celdas_transitadas.csv"  # relativa a pipeline/

# ─── Oro (04_gold.py) ───
# Trayectorias: reglas de corte de viaje de la Entrega 1
VIAJE_MAX_SALTO_H3 = 2        # salto de más de 2 celdas H3 res 7 entre reportes consecutivos
VIAJE_MAX_SILENCIO_H = 12     # más de 12 h sin reportar
VELOCIDAD_MAX_KN = 60         # velocidad implícita físicamente imposible para un buque
# Tráfico por celda y hora: H3 res 5 (~250 km²). Medido el 2023-06-01: 10 288 celdas, 70 876
# combinaciones celda×hora (res 7 serían 371 365); se ve bien en un mapa nacional. La tabla además
# separa por categoría de buque: 122 348 filas celda×hora×categoría ese día.
RES_TRAFICO = 5
# Presencia en puertos: área = anillo de 5 celdas H3 res 8 (~4 km) alrededor del punto del WPI
# (como en la Entrega 1). "Detenido" = Status fondeado/amarrado o SOG < 1 nudo; una estancia en
# el área con al menos un reporte detenido es una visita.
# Medido el 2023-06-01 frente a la regla de la Entrega 1 (Status 1/5 o nulo): falsos positivos
# navegando a ≥ 5 nudos 77 384 → 14 701; suma 1,3 M posiciones de buques detenidos con otro Status.
PUERTO_RES_H3 = 8
PUERTO_K_ANILLOS = 5
EN_PUERTO_STATUS = (1, 5)
EN_PUERTO_SOG_MAX = 1.0
# Anomalías del día (04d_gold_anomalias). Valores iniciales: se revisan con la celda de sensibilidad.
ANOM_SOG_DETENIDO_MAX = 1.0        # SOG reportado ≤ 1 nudo...
ANOM_VEL_IMPLICITA_MIN_KN = 15.0   # ...con velocidad implícita ≥ 15 nudos...
ANOM_DISTANCIA_MIN_NM = 0.5        # ...y un salto de al menos 0,5 nm (descarta el ruido del GPS de un buque amarrado)
ANOM_CLON_DISTANCIA_MIN_NM = 50.0  # mismo MMSI, dos MMSI_Real en el mismo minuto a más de 50 nm = clonación


# Mapa de intensidad del tablero (04e_gold_servicio): resolución H3 de gold.trafico_celda_dia.
# Provisional: se confirma con la celda de medición (celdas por resolución).
RES_MAPA = 6

# Histéresis de visitas: salir del área y volver al mismo puerto en menos de esto es la misma
# visita. Con "detenido en el área" como estado, la velocidad oscilando alrededor de 1 nudo
# generaba 33 182 "arribos" el 2023-06-01 (172 de un mismo buque); con estancia en el área +
# detención + esta histéresis quedan ~5 500 (máximo 10 por buque y puerto: ferris).
PUERTO_REGRESO_MAX_H = 1.0


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

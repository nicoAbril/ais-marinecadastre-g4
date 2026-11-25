# Bitácora del proyecto

El historial de commits (`git log`) registra el avance sesión a sesión. Este archivo lo
complementa con dos cosas que el historial no muestra: **qué técnica de cada clase quedó aplicada
y dónde**, y **por qué se tomó cada decisión, con la evidencia medida**.

- [Entrega 1](#entrega-1) — exploración, preguntas de negocio y almacenamiento.
- [Entrega 2](#entrega-2) — pipeline en streaming por capas, orquestación y ML.

---

# Entrega 1

Resumen de qué se tomó de las sesiones presenciales de semana 5 (Spark a escala) y semana 6
(formatos de almacenamiento), y dónde quedó en el código.

## Semana 5 — Spark a escala

- **Esquema explícito al leer** (no `inferSchema`): `01_ingesta.py` / `02_eda_silver.py`.
- **Ventanas con `partitionBy` + `orderBy`** (saldo acumulado y top N): `Trip_ID` y `Visita_ID`
  (suma acumulada por buque), desambiguación de puertos por celda H3 (`row_number()`).
- **Joins con tabla chica difundida (broadcast)**: cruces con `vessel_types` y con los puertos
  expandidos, confirmados y forzados con `.explain()` / `f.broadcast(...)`.
- **Antipatrones evitados**: nunca se trae la tabla completa al driver (`toPandas()`/`collect()`
  masivo); el join con `vessel_types` en la pregunta 3 se mueve después del `limit(10)`; cada
  `.agg()` calcula varias métricas en una sola pasada en vez de varios `groupBy` + joins.
- **`cache()`/`persist()` y UDF**: no se usaron a propósito (caché no disponible en serverless; las
  operaciones espaciales se hicieron con funciones H3 nativas, no UDF).
- **Conteo exacto vs. aproximado** y **justificar con el plan de ejecución (`.explain()`)**: base
  directa de la pregunta de negocio 1 y de la evidencia técnica en todo el punto 3.

## Semana 6 — Formatos de almacenamiento

- **Delta sobre CSV/Parquet plano**: se usa Delta en toda la capa `silver`/`gold` por ACID y
  soporte de `CLUSTER BY`; no se repitió la comparación de bytes de la clase.
- **Contrato de esquema**: `overwriteSchema=true` en cada tabla, porque el esquema cambió varias
  veces durante el desarrollo.
- **`CLUSTER BY` medido con archivos leídos**: el núcleo del Requisito 4 en
  `04_almacenamiento.py` — mismo experimento de la clase (filtrar y contar archivos), aplicado a
  `ais_cleaned` y `visitas_puerto`.
- **No se usaron** (no había caso de uso en esta entrega, sin pipeline incremental):
  *time travel*, `MERGE INTO`, `OPTIMIZE`, `VACUUM`. El `Visita_SK` (llave determinística, no un
  contador ni un UUID) ya deja la puerta abierta para un `MERGE` incremental en la Parte 2.

---

# Entrega 2

## Técnicas de clase aplicadas

### Semana 9 — Structured Streaming

| Técnica | Dónde quedó |
|---|---|
| Las "cuatro líneas fijas": `readStream` → transformación → `writeStream` → `checkpointLocation` | `02_bronze.py` |
| Auto Loader (`cloudFiles`) sobre un landing de archivos | `02_bronze.py` |
| `trigger(availableNow=True)`: streaming como ETL incremental que procesa lo pendiente y termina | `02_bronze.py` |
| El checkpoint como memoria: volver a correr no relee lo ya procesado | `02_bronze.py` (autoverificación: segunda corrida = 0 filas) |
| Ventanas y watermark | pendiente: alertas en `04_gold.py` |

### Semana 10 — Calidad y deduplicación idempotente

| Técnica | Dónde quedó |
|---|---|
| Esquema tolerante y datos rescatados (`_rescued_data`) | `02_bronze.py` (modo `rescue`) |
| `foreachBatch` + `DeltaTable.merge` para escritura idempotente por llave | pendiente: `03_silver.py` |
| Destino idempotente vs. *at-least-once* al perder el checkpoint | pendiente: evidencia en `03_silver.py` |

### Semana 12 — Orquestación y calidad declarativa

| Técnica | Dónde quedó |
|---|---|
| Volume de Unity Catalog como zona de llegadas | `00_preparacion.py`, `01_productor.py` |
| Expectations y tabla de cuarentena | pendiente: `03_silver.py` |
| Lakeflow Job con tareas, dependencias y disparador | pendiente |

## Registro de decisiones

Cada entrada: qué se decidió, qué alternativas había y qué evidencia la respaldó.

### 2026-11-20 · Modelo unificado: el resultado no depende de cómo lleguen los eventos

- **Decisión:** plata y oro deben ser función del *conjunto* de eventos recibidos, no del orden,
  del tamaño de los lotes ni de las repeticiones. El mismo código sirve para streaming y backfill.
- **Consecuencia:** unidad de idempotencia distinta por capa. Bronce: el archivo (checkpoint +
  `append`). Plata: el registro `(MMSI, H3_Macro, BaseDateTime)` con `MERGE` y un ganador
  determinístico (menos campos "no disponible" → más completo → hash como último desempate).
  Oro: recalcular desde plata las llaves afectadas.
- **Alternativa descartada:** `replaceWhere` por día en plata. Es idempotente, pero con lotes
  parciales borra lo que ya había del día.

### 2026-11-20 · Medición de duplicados y orden del CSV fuente (7 días, ~60 M filas)

- **Evidencia:** 0 filas fuera del día UTC del archivo. Duplicados en `(MMSI, BaseDateTime)`:
  1 388 exactos, 12 con la misma posición y distinto contenido, 217 en la misma `H3_Macro` con
  posición distinta, 55 en `H3_Macro` distintas (dos buques reales con el mismo MMSI). El CSV
  **no está ordenado** por tiempo (27–34 % de filas retroceden respecto a la anterior).
- **Decisiones:** la llave incluye `H3_Macro` (los 55 casos de MMSI clonado sobreviven como
  buques distintos); los duplicados reales son ~0,003 %, así que para demostrar la
  deduplicación hay que inyectarlos.

### 2026-11-21 · Recursos del workspace institucional

- **Evidencia (CLI, solo lectura):** el grupo no puede crear catálogos, esquemas ni Volumes en
  `clase10`/`clase14`; clúster con modo de acceso `USER_ISOLATION` fijado por política.
- **Decisión:** desarrollar en el workspace de la Entrega 1 con todo parametrizado por catálogo
  (`config.py`), y migrar cuando se aclare qué recursos hay para el proyecto.

### 2026-11-21 · Máscara "en el agua": invertida y en mosaicos

- **Decisión:** guardar la **tierra** (~4 M celdas H3 res 7), no el agua (decenas de millones),
  con Natural Earth 1:10m menos lagos, ríos con margen y franja costera.
- **Evidencia de rendimiento** (capa de tierra, serverless):

  | Variante | Tiempo | Celdas |
  |---|---|---|
  | Un feature por continente | 546,7 s | 3 945 572 |
  | Mosaicos de 5°, reparto después del cálculo | ~24 s | 3 945 572 |
  | Mosaicos de 5°, reparto antes del cálculo (implementado) | ~10,5 s | 3 945 572 |

  Mismas celdas en las tres; repartir **después** del cálculo caro no lo paraleliza.

### 2026-11-24 · World Port Index no disponible (HTTP 503)

- **Evidencia:** `msi.nga.mil` responde 503 desde el 2026-11-21, tanto la descarga como la
  página pública.
- **Decisión:** semilla de respaldo en el repo (`pipeline/seeds/world_port_index.csv`, el CSV
  descargado de la misma URL en la Entrega 1). Se usa solo si la NGA no responde y no hay
  snapshot previo; cuando vuelva, el pipeline compara el sha256 y actualiza solo.

### 2026-11-24 · PySpark como lenguaje base

- **Decisión:** transformaciones en la API de DataFrame (como en la Entrega 1); funciones H3 y
  espaciales nativas vía `pyspark.databricks.sql.functions`; `spark.sql` solo para DDL.

### 2026-11-25 · Modo ANSI: `try_cast` en vez de `cast`

- **Evidencia:** la primera corrida de `00_preparacion` falló con
  `CAST_INVALID_INPUT: '7950.0' cannot be cast to INT` (número de puerto del WPI con decimal).
- **Decisión:** `try_cast` + conteo de descartes. Un cambio de formato en la fuente se ve como
  descarte medible, no como una caída. Aplica a plata, donde todo llega como texto.

### 2026-11-25 · Productor: un archivo completo por día, subida atómica

- **Decisión:** el CSV de NOAA entra intacto, un archivo por día (como el parcial). El
  watermark se trabaja sobre `BaseDateTime`; los datos tardíos se simulan entregando un día
  fuera de orden.
- **Hallazgo:** en un Volume no existe renombrar (un `mv` es copiar + borrar), pero subir un
  objeto es atómico. Se eliminó la carpeta `_staging/`: el archivo se arma en el disco del
  driver y se sube de una sola vez.
- **Evidencia:** segunda corrida para el mismo día → 0 archivos entregados (idempotente).

### 2026-11-25 · Bronce: esquema del encabezado, todo texto, modo `rescue`

- **Experimento:** CSV con `LON` antes que `LAT`. Auto Loader asignó cada valor a su columna
  **por nombre**, con esquema explícito y con esquema inferido. El orden no es un riesgo.
- **Decisión:** sin esquema declarado (el encabezado define las columnas, todo `STRING`); el
  contrato vive en plata. Cambios de esquema en modo `rescue`: una columna nueva va a
  `_rescued_data` y el stream sigue; la auditoría alerta.
- **Alternativa descartada:** `addNewColumns`, que detiene el stream ante cada columna nueva.
  El enunciado del parcial pedía que el diseño *"no debe fallar"* ante cambios de columnas, y
  nuestro principio es que un cambio de esquema genera alerta, no rompe el flujo.
- **Evidencia:** 3 archivos, 17 617 817 filas, un commit de Delta por archivo; segunda corrida
  = 0 filas; `delta.appendOnly = true` verificado.

### 2026-11-25 · Reglas de calidad de plata

- **Evidencia (2023-06-01, 8 808 904 filas):** timestamp inválido 0; MMSI distinto de 9
  dígitos 6 911 (0,08 %); coordenadas nulas o fuera de rango 0; SOG = 102.3 21 946 (0,25 %);
  SOG > 60 83; COG = 360 1 490 036 (16,9 %); Heading = 511 4 871 561 (55,3 %); VesselType
  desconocido 765.
- **Decisiones:**
  - **Duras (cuarentena)**, en orden de prioridad: timestamp inválido, MMSI inválido (también
    el mal formado: el enunciado pide *"identificadores bien formados"*), coordenada nula,
    coordenada fuera de rango, en tierra.
  - **Suaves (la fila sigue con el valor anulado y una bandera):** SOG no disponible o
    implausible, COG = 360, Heading = 511, VesselType desconocido. Se anula el valor del
    sensor sin descartar una posición válida (criterio de la Entrega 1).
  - **Velocidad implícita imposible** (salto entre dos puntos): no es cuarentena en plata. La
    anomalía es de la *relación* entre dos puntos (no se sabe cuál falló) y depende del orden
    de llegada; va en oro, que recalcula con todos los puntos del día.
  - **Duplicados:** no son inválidos; el `MERGE` los resuelve y se cuentan en las métricas.

### 2026-11-25 · Calibración de la máscara "en tierra" (11,4 % → 1,25 %)

- **Evidencia del problema:** con la primera máscara (franja costera de 1 anillo), el **11,44 %**
  de las posiciones del 2023-06-01 caía "en tierra" (1 007 548 filas, 3 382 buques). No eran
  errores de GPS: el 81 % estaban detenidos (< 0,5 nudos) y las zonas principales eran puertos y
  canales: canal de Houston (252 870 filas, 428 buques), pantanos de Luisiana, Nueva Orleans,
  Port Everglades, Norfolk, Seattle, Tampa. Natural Earth a 1:10m no dibuja canales, dársenas ni
  marinas.
- **Medición de alternativas** (16 combinaciones de franja costera × zona portuaria alrededor
  de los puertos del WPI):

  | Franja costera | Zona portuaria | Filas "en tierra" | % | Buques |
  |---|---|---|---|---|
  | 1 anillo (inicial) | — | 1 007 548 | 11,44 % | 3 382 |
  | 3 anillos | — | 238 052 | 2,70 % | 872 |
  | 1 anillo | 8 anillos (~10 km) | 265 812 | 3,02 % | 1 159 |
  | **3 anillos** | **8 anillos (~10 km)** | **119 514** | **1,36 %** | **493** |
  | 4 anillos | 12 anillos (~14 km) | 87 331 | 0,99 % | 306 |

- **Decisión:** `MASCARA_K_COSTA = 3` (franja junto a cualquier agua: mar, lago o río) y
  `MASCARA_K_PUERTOS = 8`. Más allá, cada ampliación quita poco y vuelve la máscara permisiva con
  errores reales cerca de la costa. Se descartó una costa más precisa (OSM, NOAA, USGS) por costo
  de descarga y procesamiento para un caso de estudio.
- **Resultado real tras reconstruir:** máscara de 2 099 026 celdas (antes 3 207 452); día 1 con
  **109 897 filas en tierra (1,25 %), 484 buques**; 5/5 puntos de control OK (incluido el canal
  de Houston). La máscara ahora depende también de `dim_puertos`: su versión incluye el sha del
  WPI.
- **Limitación conocida:** el 1,25 % restante va a cuarentena con motivo `en_tierra` e incluye
  algo de tráfico real en canales que la escala 1:10m no resuelve. No se pierde: queda en la
  cuarentena.

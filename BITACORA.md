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

Qué se tomó de las sesiones de semana 9 (streaming), 10 (calidad) y 12 (orquestación), dónde
quedó, y las decisiones que cambiaron el diseño con su evidencia. El detalle de cada decisión
está en la sección *Decisiones* de cada notebook.

## Semana 9 — Structured Streaming

- **`readStream` → `writeStream` con checkpoint y `trigger(availableNow=True)`**: `02_bronze.py`
  (Auto Loader) y `03_silver.py`. Volver a correr procesa 0 filas (verificado en ambos).
- **Auto Loader sobre un Volume de llegadas**: `01_productor.py` sube un CSV completo por día
  (subida atómica, nombre inmutable); `02_bronze.py` lo ingiere sin esquema declarado (todo
  texto, columnas por nombre del encabezado).
- **No se usaron ventanas con watermark**: el enunciado no pide alertas en tiempo real; los datos
  tardíos se corrigen con `MERGE` en plata y recálculo de la fecha en oro.

## Semana 10 — Calidad y deduplicación idempotente

- **Esquema tolerante (`_rescued_data`)**: modo `rescue` en bronce; una columna nueva no detiene
  el stream y plata la cuenta en `filas_esquema_rescatado`.
- **`foreachBatch` + `MERGE` por llave**: `03_silver.py`, llave `(MMSI, H3_Macro,
  BaseDateTime)` con ganador determinístico (menos campos "no disponible" → más completo →
  hash). Resultado igual sin importar orden o repeticiones (modelo unificado).
- **Compuertas y cuarentena con causa**: reglas duras con prioridad (`motivo_principal`) y
  reglas suaves que anulan el valor y dejan bandera; métricas por fecha en `calidad_metricas`.

## Semana 12 — Orquestación

- **Volume de Unity Catalog como zona de llegadas**: `00_preparacion.py`, `01_productor.py`.
- **Lakeflow Job**: pendiente.

## Decisiones y hallazgos

- **Duplicados y orden de la fuente** (7 días, ~60 M filas): 0 filas fuera del día del archivo;
  duplicados reales ~0,003 % (por eso se inyectan); 55 casos de MMSI compartido entre buques
  distintos → la llave incluye `H3_Macro`. El CSV no viene ordenado por tiempo (~30 %).
- **Máscara "en el agua"**: se guarda la tierra (H3 res 7), no el agua. Calcularla por mosaicos
  de 5° bajó de 546 s a ~10 s. La primera versión mandaba el 11,4 % del día a "en tierra"
  (puertos y canales que Natural Earth 1:10m no dibuja); con franja costera de 3 anillos y
  zona portuaria de 8 quedó en 1,25 %.
- **World Port Index caído (HTTP 503)**: semilla de respaldo en el repo con el CSV de la Entrega 1.
- **Modo ANSI**: `try_cast` en vez de `cast` (el WPI trae `'7950.0'` en un campo entero).
- **`foreachBatch` no corre desde Databricks Connect** (`exec format error` del sandbox): plata se
  ejecuta dentro de Databricks; el resto también corre en local.
- **Oro batch incremental**: plata recibe `MERGE` y no admite lectura en stream. Oro recalcula
  las fechas donde `calidad_metricas` es más nueva que su `_version_silver`, más el día siguiente.
- **Tráfico en H3 res 5**: 70 876 combinaciones celda×hora el 2023-06-01 (res 7: 371 365).
- **Arribos a puerto**: la regla "detenido en el área" parpadea con la velocidad alrededor de
  1 nudo (33 182 arribos en un día, 172 de un solo buque). La Entrega 1 tenía el mismo defecto
  (máximo 209). Nueva regla: estancia en el área + al menos un reporte detenido + salir y volver
  en < 1 h es la misma visita → 5 526 arribos, máximo 10 (ferris). Se quitaron los zarpes.
- **Plata, 2023-06-01**: 17 617 817 recibidas = 8 691 916 plata + 116 814 cuarentena + 8 809 087
  duplicados (el reenvío completo). Las 8 filas inyectadas cayeron cada una en su motivo.
- **Alcance**: sin alertas con watermark ni tabla de auditoría; el monitoreo es el job más un
  chequeo de salud al final de `04_gold` (oro = plata por fecha).

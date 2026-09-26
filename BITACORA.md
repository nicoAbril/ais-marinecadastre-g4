# Técnicas de clase aplicadas en el proyecto

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

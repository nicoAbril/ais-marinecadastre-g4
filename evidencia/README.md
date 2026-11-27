# Evidencia de ejecución en Databricks

Un HTML por notebook, **ejecutado por el job** `oceanwatch_pipeline` y exportado con sus salidas
(`databricks jobs export-run <run_id_de_la_tarea> --views-to-export CODE`). Se abren en cualquier
navegador. Para cada notebook se eligió la corrida que mejor muestra lo que hace.

| Notebook | Corrida del job | Qué muestra |
|---|---|---|
| [`00_preparacion`](00_preparacion.html) | `960708225394845` | Estructura de Unity Catalog, permisos por rol y reconstrucción de la máscara al cambiar su semilla (celdas transitadas); puntos de control |
| [`01_productor`](01_productor.html) | `430513209009606` (`fecha = 2026-11-19`) | Reloj simulado (2026-11-19 → datos del 2023-06-02) y entrega atómica del CSV del día |
| [`02_bronze`](02_bronze.html) | `430513209009606` | Auto Loader en modo `rescue`, un commit por archivo y segunda corrida = 0 filas |
| [`03_silver`](03_silver.html) | `907239464863229` | Compuertas con las reglas finales, cuarentena, `MERGE` idempotente, unicidad, filas inyectadas y métricas de calidad |
| [`04_gold`](04_gold.html) | `907239464863229` | Recálculo por versión de plata de los 10 días, chequeo de salud (oro = plata) y linaje |
| [`04d_gold_anomalias`](04d_gold_anomalias.html) | `907239464863229` | Anomalías del día por tipo y caso de control |
| [`04e_gold_servicio`](04e_gold_servicio.html) | `907239464863229` | Tablas de servicio del tablero y cuadre contra oro |
| [`05_ml`](05_ml.html) | `907239464863229` | Entrenamiento del IsolationForest con MLflow, registro en Unity Catalog como `@champion` y puntuación contra la regla |

La **ejecución programada** (requisito 3) se ve en el historial del job: corrida
`366852589332398`, disparo `PERIODIC` del 2026-11-27 a las 06:00 (Bogotá), terminada con éxito.

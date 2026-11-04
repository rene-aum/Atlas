# Atlas Guide

## 1) The Two `pipelineAtlas` Versions

Atlas has two main execution notebooks. They share the same business logic (`RawAtlas`, `ProcessedAtlas`) but they are not used the same way operationally.

### A. `PipelinesConsumo/notebooks/1_pipelineAtlas.ipynb`

This is the "full" operational pipeline for Atlas consumo.

What it does, in order:

1. Bootstraps Colab/session and installs dependencies.
2. Authenticates Google Drive + Google Sheets and AWS.
3. Pulls source files from Drive local to runtime.
4. Runs raw stage with `RawAtlas` and generates `t1`..`t10` (some raw tables are currently disabled in-code).
5. Writes those raw outputs back to Drive (`raw_output_ids` map).
6. Reads raw outputs and builds processed stage with `ProcessedAtlas`, generating `tp1`..`tp14`.
7. Publishes outputs to destination sheets (`consumo_sheets_ids_dict`).
8. Sends Google Chat notification and frees memory.

Key behavior:

- This version computes core processed tables (`tp1`, `tp2`, `tp3`, `tp9`..`tp14`) in the notebook.
- Funnel-related outputs (`tp4`..`tp8`) are read from S3 latest partitions.
- It is the most complete "single-run" pipeline for consumo publication.

### B. `PipelinesConsumo/notebooks/1_pipelineAtlasDataLake.ipynb`

This is a hybrid "Data Lake assisted" version.

What it does, in order:

1. Bootstraps environment and auth (same pattern as above).
2. Downloads only a subset of source files required for local recalculation.
3. Runs a reduced raw stage (`t6`, `t7`, `t9`, `t10`).
4. Pulls `tp1`..`tp8` from S3 Silver latest partitions.
5. Recomputes only the local portion (`tp9`..`tp14`) with `ProcessedAtlas`.
6. Publishes outputs to Sheets using a folder-derived `atlasConsumoDict`.
7. Leaves notification/cleanup cells commented in current notebook version.

Key behavior:

- Optimized when most processed assets are already in Data Lake.
- Reduces runtime by avoiding full recomputation of all tables.
- Useful for refresh/update workflows rather than full rebuild workflows.

### When To Use Each

- Use `1_pipelineAtlas.ipynb` when you need an end-to-end refresh from full source ingestion to publication.
- Use `1_pipelineAtlasDataLake.ipynb` when S3 Silver already has fresh core partitions and you only need partial recompute + publish.

---

## 2) What `CasosDeUso` Is

`CasosDeUso/` is the consumption layer. These notebooks are not the canonical data pipeline; they consume Atlas outputs to answer specific business needs.

Think of them as "products" built on Atlas tables:

- Dashboards for ongoing monitoring.
- Ad hoc reports for specific requests.
- Validation/analysis notebooks for deep dives.

### Current organization

- `CasosDeUso/Dashboards/`
- `CasosDeUso/ReportesAdhoc/`
- `CasosDeUso/CasosErick/`

### Typical pattern inside a use case notebook

1. Import shared pipeline classes/utilities.
2. Read already processed Atlas outputs (`Ac*` tables).
3. Build the KPI view/report logic needed by that specific audience.
4. Publish or visualize the final result.

### Why this separation matters

- Pipeline notebooks own data standardization and business table contracts.
- Use case notebooks own presentation, slicing, and business questions.
- This avoids duplicating heavy transformation logic in each dashboard/report.

---

## 3) What `Orquestador` Does

`Orquestador/` is the notebook runner layer. It chains multiple notebooks in sequence from one entrypoint.

Current notebooks:

- `Orquestador/notebooks/orquestadorAtlas.ipynb`
- `Orquestador/notebooks/orquestadorMatutino.ipynb`

### `orquestadorAtlas.ipynb`

Purpose:

- Runs the Atlas pipeline and then selected downstream dashboards/reports in one sequence.

Current flow (high level):

1. Bootstrap/auth once.
2. Execute a curated list of notebooks with `%run`.
3. Stop on first failure (`break`) to prevent silent partial delivery.

### `orquestadorMatutino.ipynb`

Purpose:

- Runs a "morning" subset focused on business-facing updates (without running full Atlas pipeline first in the same list).

Current flow (high level):

1. Same bootstrap/auth approach.
2. Executes a selected list from `CasosDeUso`.
3. Logs elapsed time per notebook and total execution time.

### Operational role of Orquestador

- Centralizes run order.
- Gives one place to maintain "what runs together".
- Useful for scheduled or semi-automated execution in Colab-style operations.

---

## 4) `utils`: Shared Operational Toolbox

`utils/` contains reusable helpers used by both pipeline and use case notebooks.

### `utils/drive_toolbox.py`

Main responsibility: Google Drive/Sheets operations and delivery helpers.

Includes helpers for:

- downloading Drive files to local runtime
- listing files in Drive folders
- reading/writing CSV by Drive file ID
- reading/writing Google Sheets with retries
- chunked Sheet updates for large DataFrames
- sending Google Chat webhook notifications

### `utils/aws_toolbox.py`

Main responsibility: S3/Data Lake access for partitioned parquet workflows.

Includes helpers for:

- interactive AWS credential setup
- credential testing
- latest partition resolution (`year/month/day`)
- reading parquet from full path or latest partition

This module is what lets Atlas read `Silver/Atlas/*` partition outputs efficiently.

### `utils/utils.py`

Main responsibility: generic dataframe and text helpers.

Includes helpers for:

- column normalization (`process_columns`)
- flexible local read (`custom_read` for csv/xlsx tabs)
- date enrichment (`add_year_week`)
- accent/mojibake cleanup helpers

Use this file for small pure utility logic that should be shared across notebooks.

---

## Practical Mental Model

Atlas in one line:

1. `1_pipelineAtlas*` notebooks build/publish trusted Atlas tables.
2. `CasosDeUso/` notebooks consume those tables for concrete business outputs.
3. `Orquestador/` runs selected notebooks in the right order.
4. `utils/` provides the shared plumbing to make all of that reliable.


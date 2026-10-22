from datetime import datetime

import pandas as pd
import pytz

from automarket_utils.drive import (
    read_from_google_sheets,
    update_sheets_in_drive_folder,
)

try:
    from PipelinesConsumo.src.crm_config import (
        CRM_CONSUMO_OUTPUT_IDS,
        CRM_CONSUMO_SHEET_NAMES,
        CRM_EXTERNAL_SHEET_INPUTS,
        CRM_LOG_FOLDER_ID,
        CRM_RAW_LATEST_FOLDER_ID,
        CRM_RAW_SNAPSHOT_FOLDER_ID,
        CRM_SOURCE_FOLDER_ID,
    )
    from PipelinesConsumo.src.constants import mexico_tz
    from PipelinesConsumo.src.crm_logging import CrmRunLogger, safe_upload_log
    from PipelinesConsumo.src.processedCrmAtlas import ProcessedCrmAtlas
    from PipelinesConsumo.src.rawCrmAtlas import RawCrmAtlas
except ModuleNotFoundError:
    from src.crm_config import (
        CRM_CONSUMO_OUTPUT_IDS,
        CRM_CONSUMO_SHEET_NAMES,
        CRM_EXTERNAL_SHEET_INPUTS,
        CRM_LOG_FOLDER_ID,
        CRM_RAW_LATEST_FOLDER_ID,
        CRM_RAW_SNAPSHOT_FOLDER_ID,
        CRM_SOURCE_FOLDER_ID,
    )
    from src.constants import mexico_tz
    from src.crm_logging import CrmRunLogger, safe_upload_log
    from src.processedCrmAtlas import ProcessedCrmAtlas
    from src.rawCrmAtlas import RawCrmAtlas


def get_crm_run_timestamp():
    """Return the CRM run timestamp used in filenames, logs, and metadata."""
    return datetime.now(tz=pytz.timezone(mexico_tz)).strftime("%Y%m%d_%H%M%S")


def add_run_metadata(df, snapshot_timestamp):
    """
    Add lightweight lineage columns before writing a consumo output.

    The raw snapshots remain untouched; these metadata columns are only added to
    outputs written to Atlas Consumo so users can see which CRM run produced the
    current sheet contents.
    """
    return df.assign(
        run_date=pd.to_datetime(snapshot_timestamp, format="%Y%m%d_%H%M%S").strftime(
            "%Y-%m-%d"
        ),
        snapshot_timestamp=snapshot_timestamp,
    )


def read_crm_external_inputs(gc, external_sheet_inputs=None, logger=None):
    """Read the existing Atlas Consumo tables required by CRM reports.

    These tables have independent upstream pipelines.  Reading them here makes
    their use by the CRM reports explicit and records their Sheet IDs and shape
    in the CRM execution log.
    """
    external_sheet_inputs = external_sheet_inputs or CRM_EXTERNAL_SHEET_INPUTS
    external_dfs = {}

    for input_name, config in external_sheet_inputs.items():
        spreadsheet_id = config["spreadsheet_id"]
        if logger:
            logger.info(
                "consumo.read_external.start",
                input_name=input_name,
                spreadsheet_id=spreadsheet_id,
            )
        try:
            df = read_from_google_sheets(gc, spreadsheet_id)
            required_columns = set(config.get("required_columns", []))
            missing_columns = required_columns.difference(df.columns)
            if missing_columns:
                raise ValueError(
                    f"{input_name}: faltan columnas requeridas: "
                    f"{sorted(missing_columns)}"
                )
            external_dfs[input_name] = df
        except Exception as e:
            if logger:
                logger.error(
                    "consumo.read_external.failed",
                    e,
                    input_name=input_name,
                    spreadsheet_id=spreadsheet_id,
                )
            raise
        if logger:
            logger.success(
                "consumo.read_external.done",
                input_name=input_name,
                spreadsheet_id=spreadsheet_id,
                rows=len(df),
                columns=len(df.columns),
            )

    return external_dfs


def run_crm_raw_snapshot(
    drive,
    source_folder_id=CRM_SOURCE_FOLDER_ID,
    raw_snapshot_folder_id=CRM_RAW_SNAPSHOT_FOLDER_ID,
    latest_snapshot_folder_id=CRM_RAW_LATEST_FOLDER_ID,
    local_dir=".",
    report_keys=None,
    write_manifest=True,
    timestamp=None,
    log_folder_id=CRM_LOG_FOLDER_ID,
):
    """
    Download CRM source files and write timestamped raw snapshots to Drive.

    This is the raw stage of the CRM pipeline. It copies the current overwritten
    Salesforce exports from the source Drive folder into the runtime, then writes
    timestamped CSV snapshots into a dated child folder under
    raw_snapshot_folder_id. If latest_snapshot_folder_id is provided, it also
    upserts stable latest CSVs there, without timestamps in the filenames.

    A TXT log is uploaded in finally, so the log is still attempted if download
    or snapshot creation fails.

    Returns
    -------
    dict
        timestamp, downloaded source metadata, snapshot Drive IDs, manifest
        DataFrame, raw log Drive ID, and rendered log text.
    """
    timestamp = timestamp or get_crm_run_timestamp()
    logger = CrmRunLogger(
        "raw",
        timestamp,
        source_folder_id=source_folder_id,
        raw_snapshot_folder_id=raw_snapshot_folder_id,
        latest_snapshot_folder_id=latest_snapshot_folder_id,
        local_dir=local_dir,
        report_keys=report_keys,
    )
    raw = RawCrmAtlas(
        source_folder_id=source_folder_id,
        raw_snapshot_folder_id=raw_snapshot_folder_id,
        latest_snapshot_folder_id=latest_snapshot_folder_id,
        local_dir=local_dir,
    )
    raw.timestamp = timestamp

    downloaded_sources = {}
    snapshot_ids = {}
    manifest = pd.DataFrame()
    log_drive_id = None

    try:
        logger.info("raw.stage.start")
        downloaded_sources = raw.download_sources(
            drive,
            report_keys=report_keys,
            logger=logger,
        )

        if raw_snapshot_folder_id:
            snapshot_ids, manifest = raw.snapshot_sources(
                drive,
                downloaded_sources,
                raw_snapshot_folder_id=raw_snapshot_folder_id,
                latest_snapshot_folder_id=latest_snapshot_folder_id,
                report_keys=report_keys,
                timestamp=timestamp,
                write_manifest=write_manifest,
                logger=logger,
            )
        else:
            logger.warning(
                "raw.snapshot.skipped",
                reason="raw_snapshot_folder_id is not configured",
            )
            print("CRM raw snapshot skipped: raw_snapshot_folder_id is not configured.")

        logger.finish(
            downloaded_tables=len(downloaded_sources),
            snapshot_files=len([k for k in snapshot_ids.keys() if not k.startswith("_")]),
            latest_files=len(snapshot_ids.get("_latest", {})),
        )
    except Exception as e:
        logger.error("raw.stage.failed", e)
        logger.finish(status="FAILED")
        raise
    finally:
        log_drive_id = safe_upload_log(
            logger,
            drive,
            log_folder_id or raw_snapshot_folder_id,
            f"logRawCrm_{timestamp}.txt",
        )

    return {
        "timestamp": timestamp,
        "downloaded_sources": downloaded_sources,
        "snapshot_ids": snapshot_ids,
        "manifest": manifest,
        "log_drive_id": log_drive_id,
        "log_text": logger.render(),
    }


def build_crm_consumo_outputs(
    downloaded_sources,
    external_dfs,
    report_keys=None,
    logger=None,
):
    """
    Read downloaded CRM files and build DataFrames without writing to Drive.

    This is useful for tests and notebook inspection. It runs the same processing
    path used by run_crm_consumo but stops before updating Atlas Consumo sheets.
    """
    raw_reader = RawCrmAtlas()
    raw_dfs = raw_reader.read_local_sources(
        downloaded_sources,
        report_keys=report_keys,
        logger=logger,
    )
    processor = ProcessedCrmAtlas()
    return processor.build_consumo_outputs(
        raw_dfs,
        extra_dfs=external_dfs,
        logger=logger,
    )


def write_crm_consumo_outputs(
    gc,
    outputs,
    consumo_output_ids=None,
    sheet_names=None,
    output_keys=None,
    snapshot_timestamp=None,
    include_metadata=True,
    logger=None,
):
    """
    Write selected CRM outputs to Atlas Consumo Google Sheets.

    By default only the consumption-facing reports are written, while
    intermediate DataFrames remain available in the returned outputs.

    Parameters
    ----------
    gc : gspread.Client
        Authenticated Google Sheets client.
    outputs : dict
        Output DataFrames from build_crm_consumo_outputs.
    consumo_output_ids : dict, optional
        Mapping from output key to spreadsheet ID. Defaults to crm_config.py.
    output_keys : list, optional
        Subset of outputs to write. Defaults to the keys configured in
        consumo_output_ids.

    Returns
    -------
    dict
        Write summary by output key with sheet ID, sheet name, rows, and columns.
    """
    consumo_output_ids = consumo_output_ids or CRM_CONSUMO_OUTPUT_IDS
    sheet_names = sheet_names or CRM_CONSUMO_SHEET_NAMES
    output_keys = output_keys or list(consumo_output_ids.keys())
    snapshot_timestamp = snapshot_timestamp or get_crm_run_timestamp()

    written = {}
    for output_key in output_keys:
        if output_key not in outputs:
            if logger:
                logger.error(
                    "consumo.write_sheet.failed",
                    KeyError(f"Output CRM no encontrado: {output_key}"),
                    output_key=output_key,
                )
            raise KeyError(f"Output CRM no encontrado: {output_key}")
        if output_key not in consumo_output_ids:
            if logger:
                logger.error(
                    "consumo.write_sheet.failed",
                    KeyError(f"No hay spreadsheet_id configurado para {output_key}"),
                    output_key=output_key,
                )
            raise KeyError(f"No hay spreadsheet_id configurado para {output_key}")

        df_to_write = outputs[output_key]
        if include_metadata:
            df_to_write = add_run_metadata(df_to_write, snapshot_timestamp)

        sheet_name = sheet_names.get(output_key, "Hoja 1")
        if logger:
            logger.info(
                "consumo.write_sheet.start",
                output_key=output_key,
                spreadsheet_id=consumo_output_ids[output_key],
                sheet_name=sheet_name,
                rows=len(df_to_write),
                columns=len(df_to_write.columns),
            )
        try:
            update_sheets_in_drive_folder(
                gc,
                consumo_output_ids[output_key],
                sheet_name,
                df_to_write,
            )
        except Exception as e:
            if logger:
                logger.error(
                    "consumo.write_sheet.failed",
                    e,
                    output_key=output_key,
                    spreadsheet_id=consumo_output_ids[output_key],
                    sheet_name=sheet_name,
                )
            raise
        written[output_key] = {
            "spreadsheet_id": consumo_output_ids[output_key],
            "sheet_name": sheet_name,
            "rows": len(df_to_write),
            "columns": len(df_to_write.columns),
        }
        if logger:
            logger.success(
                "consumo.write_sheet.done",
                output_key=output_key,
                spreadsheet_id=consumo_output_ids[output_key],
                sheet_name=sheet_name,
                rows=len(df_to_write),
                columns=len(df_to_write.columns),
            )

    return written


def run_crm_consumo(
    gc,
    downloaded_sources,
    drive=None,
    consumo_output_ids=None,
    sheet_names=None,
    output_keys=None,
    snapshot_timestamp=None,
    write_outputs=True,
    include_metadata=True,
    log_folder_id=CRM_LOG_FOLDER_ID,
    external_dfs=None,
    external_sheet_inputs=None,
):
    """
    Build CRM consumo outputs and optionally write them to Google Sheets.

    This is the process/consumo stage. It reads local files downloaded by the raw
    stage, applies ProcessedCrmAtlas transformations, and optionally updates the
    configured Atlas Consumo sheets.

    A TXT log is uploaded in finally, so the log is still attempted if reading,
    processing, or writing fails.

    Returns
    -------
    dict
        Built output DataFrames, write summary, consumo log Drive ID, and
        rendered log text.
    """
    snapshot_timestamp = snapshot_timestamp or get_crm_run_timestamp()
    logger = CrmRunLogger(
        "consumo",
        snapshot_timestamp,
        output_keys=output_keys,
        write_outputs=write_outputs,
        include_metadata=include_metadata,
    )

    outputs = {}
    written = {}
    log_drive_id = None

    try:
        logger.info("consumo.stage.start")
        external_dfs = external_dfs or read_crm_external_inputs(
            gc,
            external_sheet_inputs=external_sheet_inputs,
            logger=logger,
        )
        outputs = build_crm_consumo_outputs(
            downloaded_sources,
            external_dfs=external_dfs,
            logger=logger,
        )

        if write_outputs:
            written = write_crm_consumo_outputs(
                gc,
                outputs,
                consumo_output_ids=consumo_output_ids,
                sheet_names=sheet_names,
                output_keys=output_keys,
                snapshot_timestamp=snapshot_timestamp,
                include_metadata=include_metadata,
                logger=logger,
            )
        else:
            logger.warning(
                "consumo.write_outputs.skipped",
                reason="write_outputs=False",
            )

        logger.finish(
            outputs_built=len(outputs),
            outputs_written=len(written),
        )
    except Exception as e:
        logger.error("consumo.stage.failed", e)
        logger.finish(status="FAILED")
        raise
    finally:
        log_drive_id = safe_upload_log(
            logger,
            drive,
            log_folder_id,
            f"logConsumoCrm_{snapshot_timestamp}.txt",
        )

    return {
        "outputs": outputs,
        "written": written,
        "log_drive_id": log_drive_id,
        "log_text": logger.render(),
    }


def run_crm_pipeline(
    drive,
    gc,
    source_folder_id=CRM_SOURCE_FOLDER_ID,
    raw_snapshot_folder_id=CRM_RAW_SNAPSHOT_FOLDER_ID,
    latest_snapshot_folder_id=CRM_RAW_LATEST_FOLDER_ID,
    consumo_output_ids=None,
    sheet_names=None,
    output_keys=None,
    local_dir=".",
    write_outputs=True,
    include_metadata=True,
    log_folder_id=CRM_LOG_FOLDER_ID,
    external_dfs=None,
    external_sheet_inputs=None,
):
    """
    Notebook-friendly CRM pipeline:
    1. download overwritten Salesforce exports
    2. write raw snapshots for traceability
    3. optionally upsert latest raw snapshots with stable filenames
    4. process CRM consumption outputs
    5. optionally write selected outputs to Atlas Consumo

    This is the single function most notebooks should call. Use write_outputs=False
    for dry runs that build and preview outputs without overwriting Sheets.

    Returns
    -------
    dict
        The shared timestamp plus nested raw and consumo stage results.
    """
    timestamp = get_crm_run_timestamp()
    raw_result = run_crm_raw_snapshot(
        drive=drive,
        source_folder_id=source_folder_id,
        raw_snapshot_folder_id=raw_snapshot_folder_id,
        latest_snapshot_folder_id=latest_snapshot_folder_id,
        local_dir=local_dir,
        timestamp=timestamp,
        log_folder_id=log_folder_id,
    )
    consumo_result = run_crm_consumo(
        gc=gc,
        downloaded_sources=raw_result["downloaded_sources"],
        drive=drive,
        consumo_output_ids=consumo_output_ids,
        sheet_names=sheet_names,
        output_keys=output_keys,
        snapshot_timestamp=timestamp,
        write_outputs=write_outputs,
        include_metadata=include_metadata,
        log_folder_id=log_folder_id or raw_snapshot_folder_id,
        external_dfs=external_dfs,
        external_sheet_inputs=external_sheet_inputs,
    )

    return {
        "timestamp": timestamp,
        "raw": raw_result,
        "consumo": consumo_result,
    }

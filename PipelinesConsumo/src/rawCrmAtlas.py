import os
from datetime import datetime

import pandas as pd
import pytz
from unidecode import unidecode

from automarket_utils.drive import (
    create_csv_file_in_drive_folder,
    from_drive_to_local,
    list_file_ids_for_drive_folder,
)

try:
    from PipelinesConsumo.src.crm_config import (
        CRM_RAW_SNAPSHOT_FOLDER_ID,
        CRM_SOURCE_FILES,
        CRM_SOURCE_FOLDER_ID,
    )
    from PipelinesConsumo.src.constants import mexico_tz
except ModuleNotFoundError:
    from src.crm_config import (
        CRM_RAW_SNAPSHOT_FOLDER_ID,
        CRM_SOURCE_FILES,
        CRM_SOURCE_FOLDER_ID,
    )
    from src.constants import mexico_tz


class RawCrmAtlas:
    """Drive ingestion and raw snapshots for temporary Salesforce CRM exports."""

    def __init__(
        self,
        source_folder_id=CRM_SOURCE_FOLDER_ID,
        raw_snapshot_folder_id=CRM_RAW_SNAPSHOT_FOLDER_ID,
        local_dir=".",
        source_files=None,
    ):
        self.source_folder_id = source_folder_id
        self.raw_snapshot_folder_id = raw_snapshot_folder_id
        self.local_dir = local_dir
        self.source_files = source_files or CRM_SOURCE_FILES
        self.timestamp = datetime.now(
            tz=pytz.timezone(mexico_tz)
        ).strftime("%Y%m%d_%H%M%S")

    @staticmethod
    def _normalize_filename(filename):
        return unidecode(str(filename)).strip().lower()

    def _resolve_report_keys(self, report_keys=None):
        return list(report_keys) if report_keys is not None else list(self.source_files.keys())

    def _find_drive_file(self, file_id_dict, expected_filename):
        expected_norm = self._normalize_filename(expected_filename)
        for title, file_id in file_id_dict.items():
            if title == expected_filename:
                return title, file_id
        for title, file_id in file_id_dict.items():
            if self._normalize_filename(title) == expected_norm:
                return title, file_id
        raise FileNotFoundError(
            f"No se encontro {expected_filename!r} en el folder CRM de Drive."
        )

    def list_source_files(self, drive):
        return list_file_ids_for_drive_folder(drive, self.source_folder_id)

    def download_sources(self, drive, report_keys=None, logger=None):
        """Download configured CRM source files and return metadata by table key."""
        if logger:
            logger.info(
                "raw.list_sources.start",
                source_folder_id=self.source_folder_id,
            )
        file_id_dict = self.list_source_files(drive)
        if logger:
            logger.success(
                "raw.list_sources.done",
                source_files_found=len(file_id_dict),
            )
        downloaded = {}

        os.makedirs(self.local_dir, exist_ok=True)

        for table_name in self._resolve_report_keys(report_keys):
            cfg = self.source_files[table_name]
            try:
                if logger:
                    logger.info(
                        "raw.download.start",
                        table_name=table_name,
                        expected_file=cfg["source_file"],
                    )
                source_title, source_id = self._find_drive_file(
                    file_id_dict,
                    cfg["source_file"],
                )
                local_path = os.path.join(self.local_dir, cfg["source_file"])

                print(f"Downloading CRM source {source_title} -> {local_path}")
                from_drive_to_local(drive, source_id, local_path)

                downloaded[table_name] = {
                    "source_title": source_title,
                    "source_id": source_id,
                    "local_path": local_path,
                }
                if logger:
                    logger.success(
                        "raw.download.done",
                        table_name=table_name,
                        source_title=source_title,
                        source_id=source_id,
                        local_path=local_path,
                    )
            except Exception as e:
                if logger:
                    logger.error(
                        "raw.download.failed",
                        e,
                        table_name=table_name,
                        expected_file=cfg["source_file"],
                    )
                raise

        return downloaded

    def read_local_sources(self, downloaded_sources, report_keys=None, logger=None):
        """Read downloaded Excel files into DataFrames by configured table key."""
        raw_dfs = {}
        for table_name in self._resolve_report_keys(report_keys):
            cfg = self.source_files[table_name]
            local_path = downloaded_sources[table_name]["local_path"]
            try:
                if logger:
                    logger.info(
                        "raw.read_local.start",
                        table_name=table_name,
                        local_path=local_path,
                        sheet_name=cfg.get("sheet_name", "Sheet1"),
                    )
                raw_dfs[table_name] = pd.read_excel(
                    local_path,
                    sheet_name=cfg.get("sheet_name", "Sheet1"),
                )
                if logger:
                    logger.success(
                        "raw.read_local.done",
                        table_name=table_name,
                        rows=len(raw_dfs[table_name]),
                        columns=len(raw_dfs[table_name].columns),
                    )
            except Exception as e:
                if logger:
                    logger.error(
                        "raw.read_local.failed",
                        e,
                        table_name=table_name,
                        local_path=local_path,
                        sheet_name=cfg.get("sheet_name", "Sheet1"),
                    )
                raise
        return raw_dfs

    def snapshot_sources(
        self,
        drive,
        downloaded_sources,
        raw_snapshot_folder_id=None,
        report_keys=None,
        timestamp=None,
        write_manifest=True,
        logger=None,
    ):
        """
        Write raw CRM snapshots as timestamped CSVs in Drive.

        The raw DataFrames are not modified. Traceability metadata is kept in a
        separate manifest so the snapshot remains a faithful picture.
        """
        folder_id = raw_snapshot_folder_id or self.raw_snapshot_folder_id
        if not folder_id:
            raise ValueError(
                "raw_snapshot_folder_id es requerido para escribir snapshots CRM."
            )

        timestamp = timestamp or self.timestamp
        manifest_rows = []
        snapshot_ids = {}

        for table_name in self._resolve_report_keys(report_keys):
            cfg = self.source_files[table_name]
            source_info = downloaded_sources[table_name]
            try:
                if logger:
                    logger.info(
                        "raw.snapshot.start",
                        table_name=table_name,
                        local_path=source_info["local_path"],
                        raw_snapshot_folder_id=folder_id,
                    )
                rawdf = pd.read_excel(
                    source_info["local_path"],
                    sheet_name=cfg.get("sheet_name", "Sheet1"),
                )
                snapshot_filename = f"{timestamp}_{cfg['raw_snapshot_name']}.csv"
                snapshot_id = create_csv_file_in_drive_folder(
                    drive,
                    folder_id,
                    rawdf,
                    snapshot_filename,
                )

                snapshot_ids[table_name] = snapshot_id
                manifest_rows.append(
                    {
                        "table_name": table_name,
                        "source_title": source_info["source_title"],
                        "source_id": source_info["source_id"],
                        "local_path": source_info["local_path"],
                        "snapshot_filename": snapshot_filename,
                        "snapshot_id": snapshot_id,
                        "snapshot_timestamp": timestamp,
                        "rows": len(rawdf),
                        "columns": len(rawdf.columns),
                    }
                )
                if logger:
                    logger.success(
                        "raw.snapshot.done",
                        table_name=table_name,
                        snapshot_filename=snapshot_filename,
                        snapshot_id=snapshot_id,
                        rows=len(rawdf),
                        columns=len(rawdf.columns),
                    )
            except Exception as e:
                if logger:
                    logger.error(
                        "raw.snapshot.failed",
                        e,
                        table_name=table_name,
                        local_path=source_info.get("local_path"),
                    )
                raise

        manifest = pd.DataFrame(manifest_rows)

        if write_manifest:
            manifest_filename = f"{timestamp}_RawCrmManifest.csv"
            if logger:
                logger.info(
                    "raw.manifest.start",
                    manifest_filename=manifest_filename,
                    rows=len(manifest),
                    columns=len(manifest.columns),
                )
            try:
                manifest_id = create_csv_file_in_drive_folder(
                    drive,
                    folder_id,
                    manifest,
                    manifest_filename,
                )
            except Exception as e:
                if logger:
                    logger.error(
                        "raw.manifest.failed",
                        e,
                        manifest_filename=manifest_filename,
                    )
                raise
            snapshot_ids["_manifest"] = manifest_id
            if logger:
                logger.success(
                    "raw.manifest.done",
                    manifest_filename=manifest_filename,
                    manifest_id=manifest_id,
                )

        return snapshot_ids, manifest

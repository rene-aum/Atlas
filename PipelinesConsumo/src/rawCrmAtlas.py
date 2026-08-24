import os
from datetime import datetime

import pandas as pd
import pytz
from unidecode import unidecode

from automarket_utils.drive import (
    create_csv_file_in_drive_folder,
    from_drive_to_local,
    list_file_ids_for_drive_folder,
    write_csv_to_drive,
)

try:
    from automarket_utils.drive import create_folder_in_drive_folder
except ImportError:
    def create_folder_in_drive_folder(drive, folder_name, parent_folder_id):
        """Fallback for older automarket_utils versions without folder creation."""
        folder_metadata = {
            "title": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [{"id": parent_folder_id}],
        }
        folder = drive.CreateFile(folder_metadata)
        folder.Upload()
        print("Created folder ID:", folder["id"])
        return folder["id"]

try:
    from PipelinesConsumo.src.crm_config import (
        CRM_RAW_LATEST_FOLDER_ID,
        CRM_RAW_SNAPSHOT_FOLDER_ID,
        CRM_SOURCE_FILES,
        CRM_SOURCE_FOLDER_ID,
        CSV_ENCODING
    )
    from PipelinesConsumo.src.constants import mexico_tz
except ModuleNotFoundError:
    from src.crm_config import (
        CRM_RAW_LATEST_FOLDER_ID,
        CRM_RAW_SNAPSHOT_FOLDER_ID,
        CRM_SOURCE_FILES,
        CRM_SOURCE_FOLDER_ID,
        CSV_ENCODING,
    )
    from src.constants import mexico_tz


class RawCrmAtlas:
    """Drive ingestion and raw snapshots for temporary Salesforce CRM exports."""

    def __init__(
        self,
        source_folder_id=CRM_SOURCE_FOLDER_ID,
        raw_snapshot_folder_id=CRM_RAW_SNAPSHOT_FOLDER_ID,
        latest_snapshot_folder_id=CRM_RAW_LATEST_FOLDER_ID,
        local_dir=".",
        source_files=None,
    ):
        """
        Store the Drive folders, local download folder, and CRM file registry.

        Parameters
        ----------
        source_folder_id : str
            Drive folder where the temporary Salesforce exports are uploaded.
        raw_snapshot_folder_id : str
            Parent Drive folder where dated raw snapshot folders are created.
        latest_snapshot_folder_id : str
            Drive folder where stable latest raw CSVs are overwritten/upserted.
        local_dir : str
            Local directory used to download the Salesforce Excel files before
            reading or snapshotting them.
        source_files : dict, optional
            CRM source registry. Defaults to CRM_SOURCE_FILES from crm_config.py.
        """
        self.source_folder_id = source_folder_id
        self.raw_snapshot_folder_id = raw_snapshot_folder_id
        self.latest_snapshot_folder_id = latest_snapshot_folder_id
        self.local_dir = local_dir
        self.source_files = source_files or CRM_SOURCE_FILES
        self.timestamp = datetime.now(
            tz=pytz.timezone(mexico_tz)
        ).strftime("%Y%m%d_%H%M%S")

    @staticmethod
    def _normalize_filename(filename):
        """Normalize a Drive filename for accent-insensitive exact matching."""
        return unidecode(str(filename)).strip().lower()

    def _resolve_report_keys(self, report_keys=None):
        """
        Return the CRM table keys that this run should process.

        If report_keys is None, all configured tables are processed. Passing a
        list is useful for isolated tests, for example only ["pedidos"].
        """
        return list(report_keys) if report_keys is not None else list(self.source_files.keys())

    def _find_drive_file(self, file_id_dict, expected_filename):
        """
        Find a configured source file inside a Drive listing.

        The lookup first tries the exact filename and then retries with accents
        removed. This handles files like Creditos/Créditos without requiring
        the config to carry every possible encoding variant.
        """
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

    @staticmethod
    def _snapshot_folder_name(timestamp):
        """
        Build the dated child folder name for one raw CRM run.

        Expected timestamps use YYYYMMDD_HHMMSS. If a custom timestamp is passed
        during a manual test, the raw string is still used instead of failing.
        """
        try:
            date_tag = datetime.strptime(timestamp, "%Y%m%d_%H%M%S").strftime(
                "%Y-%m-%d_%H%M%S"
            )
        except ValueError:
            date_tag = str(timestamp)
        return f"crm_raw_{date_tag}"

    def list_source_files(self, drive):
        """Return a {Drive title: Drive file id} map for the CRM source folder."""
        return list_file_ids_for_drive_folder(drive, self.source_folder_id)

    @staticmethod
    def _upsert_latest_csv(drive, folder_id, df, filename):
        """
        Overwrite a stable latest CSV in Drive, creating it if it does not exist.

        The historical snapshot path always creates timestamped files. The latest
        path keeps one predictable filename per table for users or pipelines that
        only need the most recent raw picture.
        """
        file_id_dict = list_file_ids_for_drive_folder(drive, folder_id)
        if filename in file_id_dict:
            file_id = file_id_dict[filename]
            write_csv_to_drive(drive, file_id, df)
            return file_id
        return create_csv_file_in_drive_folder(drive, folder_id, df, filename)

    def download_sources(self, drive, report_keys=None, logger=None):
        """
        Download the configured Salesforce exports from Drive to local files.

        Returns
        -------
        dict
            Metadata by CRM table key. Each entry includes the source Drive
            title, source Drive file ID, and local path written.

        Notes
        -----
        This method does not process or snapshot the files. It only copies the
        current overwritten Drive exports into the notebook/runtime filesystem.
        """
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
        """
        Read downloaded Salesforce Excel files into raw DataFrames.

        Parameters
        ----------
        downloaded_sources : dict
            Output from download_sources.
        report_keys : list, optional
            Subset of CRM table keys to read.
        logger : CrmRunLogger, optional
            Logger used to record table-level read success/failure.

        Returns
        -------
        dict
            Raw pandas DataFrames keyed by CRM table name.
        """
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
                raw_dfs[table_name] = pd.read_csv(
                    local_path,
                    encoding=CSV_ENCODING,
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
        latest_snapshot_folder_id=None,
        report_keys=None,
        timestamp=None,
        write_manifest=True,
        logger=None,
    ):
        """
        Write raw CRM snapshots as timestamped CSVs in a dated Drive folder.

        The raw DataFrames are not modified. Traceability metadata is kept in a
        separate manifest so the snapshot remains a faithful picture.

        Flow
        ----
        1. Create a child folder inside raw_snapshot_folder_id using the run date.
        2. Re-read each downloaded Excel file exactly as raw input.
        3. Upload each raw table as a CSV into the child folder.
        4. Upsert a stable latest CSV in latest_snapshot_folder_id when provided.
        5. Upload a manifest CSV with source IDs, snapshot IDs, row counts, and
           the child folder ID.

        Returns
        -------
        tuple
            (snapshot_ids, manifest). snapshot_ids contains one Drive file ID per
            CRM table, plus _folder and _folder_name for the dated child folder.
        """
        parent_folder_id = raw_snapshot_folder_id or self.raw_snapshot_folder_id
        latest_folder_id = latest_snapshot_folder_id or self.latest_snapshot_folder_id
        if not parent_folder_id:
            raise ValueError(
                "raw_snapshot_folder_id es requerido para escribir snapshots CRM."
            )

        timestamp = timestamp or self.timestamp
        run_folder_name = self._snapshot_folder_name(timestamp)
        if logger:
            logger.info(
                "raw.snapshot_folder.start",
                parent_folder_id=parent_folder_id,
                run_folder_name=run_folder_name,
            )
        try:
            folder_id = create_folder_in_drive_folder(
                drive,
                run_folder_name,
                parent_folder_id,
            )
        except Exception as e:
            if logger:
                logger.error(
                    "raw.snapshot_folder.failed",
                    e,
                    parent_folder_id=parent_folder_id,
                    run_folder_name=run_folder_name,
                )
            raise
        if logger:
            logger.success(
                "raw.snapshot_folder.done",
                parent_folder_id=parent_folder_id,
                run_folder_name=run_folder_name,
                run_folder_id=folder_id,
            )

        manifest_rows = []
        snapshot_ids = {
            "_folder": folder_id,
            "_folder_name": run_folder_name,
            "_latest": {},
        }

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
                latest_filename = f"{cfg['raw_snapshot_name']}.csv"
                latest_id = None
                if latest_folder_id:
                    if logger:
                        logger.info(
                            "raw.latest_snapshot.start",
                            table_name=table_name,
                            latest_snapshot_folder_id=latest_folder_id,
                            latest_filename=latest_filename,
                        )
                    try:
                        latest_id = self._upsert_latest_csv(
                            drive,
                            latest_folder_id,
                            rawdf,
                            latest_filename,
                        )
                    except Exception as e:
                        if logger:
                            logger.error(
                                "raw.latest_snapshot.failed",
                                e,
                                table_name=table_name,
                                latest_snapshot_folder_id=latest_folder_id,
                                latest_filename=latest_filename,
                            )
                        raise
                    snapshot_ids["_latest"][table_name] = latest_id
                    if logger:
                        logger.success(
                            "raw.latest_snapshot.done",
                            table_name=table_name,
                            latest_snapshot_folder_id=latest_folder_id,
                            latest_filename=latest_filename,
                            latest_id=latest_id,
                        )

                manifest_rows.append(
                    {
                        "table_name": table_name,
                        "source_title": source_info["source_title"],
                        "source_id": source_info["source_id"],
                        "local_path": source_info["local_path"],
                        "snapshot_filename": snapshot_filename,
                        "snapshot_id": snapshot_id,
                        "snapshot_folder_name": run_folder_name,
                        "snapshot_folder_id": folder_id,
                        "latest_filename": latest_filename,
                        "latest_id": latest_id,
                        "latest_folder_id": latest_folder_id,
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
            latest_manifest_filename = "RawCrmManifest.csv"
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
            if latest_folder_id:
                if logger:
                    logger.info(
                        "raw.latest_manifest.start",
                        latest_snapshot_folder_id=latest_folder_id,
                        latest_manifest_filename=latest_manifest_filename,
                    )
                try:
                    latest_manifest_id = self._upsert_latest_csv(
                        drive,
                        latest_folder_id,
                        manifest,
                        latest_manifest_filename,
                    )
                except Exception as e:
                    if logger:
                        logger.error(
                            "raw.latest_manifest.failed",
                            e,
                            latest_manifest_filename=latest_manifest_filename,
                            latest_snapshot_folder_id=latest_folder_id,
                        )
                    raise
                snapshot_ids["_latest"]["_manifest"] = latest_manifest_id
                if logger:
                    logger.success(
                        "raw.latest_manifest.done",
                        latest_snapshot_folder_id=latest_folder_id,
                        latest_manifest_filename=latest_manifest_filename,
                        latest_manifest_id=latest_manifest_id,
                    )
            if logger:
                logger.success(
                    "raw.manifest.done",
                    manifest_filename=manifest_filename,
                    manifest_id=manifest_id,
                )

        return snapshot_ids, manifest

import traceback
from datetime import datetime
import pytz

try:
    from PipelinesConsumo.src.constants import mexico_tz
except ModuleNotFoundError:
    from src.constants import mexico_tz


class CrmRunLogger:
    """
    In-memory text logger for one CRM pipeline stage.

    The logger stores structured events during a run, then renders one readable
    TXT file for Drive. It is intentionally small because the notebook needs a
    durable artifact more than a full logging framework configuration.
    """

    def __init__(self, stage, timestamp, **context):
        """
        Create a logger for one stage such as raw or consumo.

        Parameters
        ----------
        stage : str
            Stage name displayed in the log header and finish event.
        timestamp : str
            Shared CRM run timestamp used in log filenames.
        **context
            Static context written at the top of the log, for example folder IDs
            or output keys selected for the run.
        """
        self.stage = stage
        self.timestamp = timestamp
        self.context = context
        self.events = []
        self.started_at = self._now()
        self.finished_at = None
        self.status = "RUNNING"
        self.log_drive_id = None

    @staticmethod
    def _now():
        """Return a Mexico City timestamp for individual log events."""
        return datetime.now(tz=pytz.timezone(mexico_tz)).strftime(
            "%Y-%m-%d %H:%M:%S %Z%z"
        )

    def event(self, level, step, **details):
        """
        Append one structured event to the in-memory log.

        level is a simple label like INFO, SUCCESS, WARNING, ERROR, or FINISH.
        step is a dot-separated name that points to the exact pipeline section.
        """
        self.events.append(
            {
                "time": self._now(),
                "level": level,
                "step": step,
                "details": details,
            }
        )

    def info(self, step, **details):
        """Record an informational event."""
        self.event("INFO", step, **details)

    def success(self, step, **details):
        """Record a successful step with optional row/file metadata."""
        self.event("SUCCESS", step, **details)

    def warning(self, step, **details):
        """Record a non-fatal issue, such as intentionally skipped writes."""
        self.event("WARNING", step, **details)

    def error(self, step, error, **details):
        """
        Record a failure and attach exception details.

        If called inside an except block, the Python traceback is included. If
        called for a deliberate validation guard, only the error type/message is
        written.
        """
        self.status = "FAILED"
        tb = traceback.format_exc()
        if tb.strip() == "NoneType: None":
            tb = ""
        details.update(
            {
                "error_type": type(error).__name__,
                "error_message": str(error),
                "traceback": tb,
            }
        )
        self.event("ERROR", step, **details)

    def finish(self, status=None, **details):
        """
        Mark the stage complete and append a final summary event.

        If no explicit status is provided, the run becomes SUCCESS unless an
        earlier error already marked it FAILED.
        """
        if status:
            self.status = status
        elif self.status == "RUNNING":
            self.status = "SUCCESS"
        self.finished_at = self._now()
        self.event("FINISH", f"{self.stage}.finish", status=self.status, **details)

    def render(self):
        """
        Convert the stored context and events into a readable TXT log.

        The output is intentionally plain text so it can be opened directly in
        Drive after a failed Colab/orchestrator run.
        """
        lines = [
            f"CRM {self.stage} execution log",
            f"timestamp: {self.timestamp}",
            f"status: {self.status}",
            f"started_at: {self.started_at}",
            f"finished_at: {self.finished_at or ''}",
            "",
            "context:",
        ]

        if self.context:
            for key, value in self.context.items():
                lines.append(f"- {key}: {value}")
        else:
            lines.append("- none")

        lines.extend(["", "events:"])
        for idx, event in enumerate(self.events, start=1):
            lines.append(f"{idx}. [{event['time']}] {event['level']} {event['step']}")
            for key, value in event["details"].items():
                if key == "traceback" and value:
                    lines.append("   traceback:")
                    for tb_line in str(value).rstrip().splitlines():
                        lines.append(f"     {tb_line}")
                else:
                    lines.append(f"   {key}: {value}")

        return "\n".join(lines) + "\n"

    def upload_to_drive(self, drive, folder_id, filename):
        """
        Upload the rendered TXT log to a Drive folder.

        Returns the created Drive file ID. If folder_id is missing, the log is
        printed locally and no upload is attempted.
        """
        if not folder_id:
            print(f"CRM log not uploaded because folder_id is not configured: {filename}")
            print(self.render())
            return None

        file_metadata = {
            "title": filename,
            "mimeType": "text/plain",
            "parents": [{"id": folder_id}],
        }
        file = drive.CreateFile(file_metadata)
        file.SetContentString(self.render())
        file.Upload()
        self.log_drive_id = file["id"]
        print(f"Uploaded CRM log {filename}: {self.log_drive_id}")
        return self.log_drive_id


def safe_upload_log(logger, drive, folder_id, filename):
    """
    Best-effort Drive upload for a CRM stage log.

    This helper must not raise: it runs from finally blocks, where raising a log
    upload error would hide the original pipeline failure that we actually need
    to debug.
    """
    if drive is None:
        print(f"CRM log not uploaded because drive is not available: {filename}")
        print(logger.render())
        return None
    try:
        return logger.upload_to_drive(drive, folder_id, filename)
    except Exception as log_error:
        print(f"WARNING: no se pudo subir el log CRM {filename}: {log_error}")
        print(logger.render())
        return None

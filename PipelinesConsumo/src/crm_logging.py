import traceback
from datetime import datetime
import pytz

try:
    from PipelinesConsumo.src.constants import mexico_tz
except ModuleNotFoundError:
    from src.constants import mexico_tz


class CrmRunLogger:
    """Small text logger for CRM pipeline runs."""

    def __init__(self, stage, timestamp, **context):
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
        return datetime.now(tz=pytz.timezone(mexico_tz)).strftime(
            "%Y-%m-%d %H:%M:%S %Z%z"
        )

    def event(self, level, step, **details):
        self.events.append(
            {
                "time": self._now(),
                "level": level,
                "step": step,
                "details": details,
            }
        )

    def info(self, step, **details):
        self.event("INFO", step, **details)

    def success(self, step, **details):
        self.event("SUCCESS", step, **details)

    def warning(self, step, **details):
        self.event("WARNING", step, **details)

    def error(self, step, error, **details):
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
        if status:
            self.status = status
        elif self.status == "RUNNING":
            self.status = "SUCCESS"
        self.finished_at = self._now()
        self.event("FINISH", f"{self.stage}.finish", status=self.status, **details)

    def render(self):
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
    """Upload a log without hiding the original pipeline error."""
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

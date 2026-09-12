"""Logging setup. Plain text by default because the first audience for these
logs is a person reading `docker compose logs`; JSON when something downstream
is collecting them."""

from __future__ import annotations

import json
import logging
import sys

from duckflow.config import settings

_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", (), None)))


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure(component: str = "duckflow") -> None:
    cfg = settings()
    handler = logging.StreamHandler(sys.stdout)
    if cfg.log_json:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(f"%(asctime)s %(levelname)-5s [{component}] %(name)s: %(message)s",
                              datefmt="%H:%M:%S")
        )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(cfg.log_level.upper())
    # The SDK's replay chatter is useful exactly once, when you are learning
    # what replay is. Turn it up with LOG_LEVEL=DEBUG.
    logging.getLogger("temporalio").setLevel(logging.WARNING)

from __future__ import annotations

import logging

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(message)s"


def _build_logger() -> logging.Logger:
    log = logging.getLogger("pafc_db_tkts")
    if log.handlers:
        return log

    log.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    log.addHandler(handler)
    log.propagate = False
    return log


log = _build_logger()

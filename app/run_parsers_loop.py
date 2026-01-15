# app/run_parsers_loop.py
import os
import time
import logging

from app.stats import parse_sanctum, parse_solscan

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("parsers")

INTERVAL = int(os.getenv("PARSER_INTERVAL_SEC", "300"))
RUN_SANCTUM = os.getenv("RUN_SANCTUM", "1") == "1"
RUN_SOLSCAN = os.getenv("RUN_SOLSCAN", "1") == "1"


def main() -> None:
    log.info("[loop] started interval=%ss sanctum=%s solscan=%s", INTERVAL, RUN_SANCTUM, RUN_SOLSCAN)

    while True:
        if RUN_SANCTUM:
            try:
                log.info("[sanctum] start")
                res = parse_sanctum()
                log.info("[sanctum] ok %s", res)
            except Exception:
                log.exception("[sanctum] ERROR")

        if RUN_SOLSCAN:
            try:
                log.info("[solscan] start")
                res = parse_solscan(limit_rows=20)
                log.info("[solscan] ok %s", res)
            except Exception:
                log.exception("[solscan] ERROR")

        time.sleep(INTERVAL)


if name == "main":
    main()

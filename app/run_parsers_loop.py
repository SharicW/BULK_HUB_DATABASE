# app/run_parsers_loop.py
import os
import time
import logging

from app.stats import parse_sanctum, parse_solscan

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("parsers")

INTERVAL = int(os.getenv("PARSER_INTERVAL_SEC", "300"))
RUN_SANCTUM = os.getenv("RUN_SANCTUM", "1") == "1"
RUN_SOLSCAN = os.getenv("RUN_SOLSCAN", "1") == "1"


def main() -> None:
    logger.info("[loop] started interval=%ss", INTERVAL)

    while True:
        if RUN_SANCTUM:
            logger.info("[sanctum] start")
            try:
                res = parse_sanctum()
                logger.info("[sanctum] ok %s", res)
            except Exception:
                logger.exception("[sanctum] ERROR")

        if RUN_SOLSCAN:
            logger.info("[solscan] start")
            try:
                res = parse_solscan(limit_rows=25)
                logger.info("[solscan] ok %s", res)
            except Exception:
                logger.exception("[solscan] ERROR")

        time.sleep(INTERVAL)


if name == "main":
    main()

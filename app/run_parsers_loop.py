# app/run_parsers_loop.py
import os
import time
import logging

from app.stats import parse_sanctum, parse_solscan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("parsers")

INTERVAL = int(os.getenv("PARSER_INTERVAL_SEC", "300"))

def main():
    logger.info("[loop] started interval=%ss", INTERVAL)

    while True:
        try:
            logger.info("[sanctum] start")
            res = parse_sanctum()
            logger.info("[sanctum] ok %s", res)
        except Exception:
            logger.exception("[sanctum] ERROR")

        try:
            logger.info("[solscan] start")
            res = parse_solscan(limit_rows=25)
            logger.info("[solscan] ok %s", res)
        except Exception:
            logger.exception("[solscan] ERROR")

        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()

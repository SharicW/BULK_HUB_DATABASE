import os
import time
import signal
import logging

from stats import parse_solscan

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("solscan_worker")

STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True
    logger.info("stop signal received (%s) -> stopping...", signum)


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)


def main() -> None:
    interval = int(os.getenv("SOLSCAN_INTERVAL_SEC", "300"))
    limit_rows = int(os.getenv("SOLSCAN_LIMIT_ROWS", "25"))
    run_once = os.getenv("SOLSCAN_RUN_ONCE", "0") == "1"

    logger.info("[solscan] worker started interval=%ss limit_rows=%s run_once=%s", interval, limit_rows, run_once)

    while not STOP:
        try:
            logger.info("[solscan] start")
            res = parse_solscan(limit_rows=limit_rows)
            logger.info("[solscan] ok %s", res)
        except Exception:
            logger.exception("[solscan] ERROR")

        if run_once:
            break

        for _ in range(interval):
            if STOP:
                break
            time.sleep(1)

    logger.info("[solscan] worker stopped")


if __name__ == "__main__":
    main()

import os
import time
import signal
import logging
from typing import Optional

from app.stats import parse_sanctum

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("sanctum_worker")

STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True
    logger.info("stop signal received (%s) -> stopping...", signum)


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)


def main() -> None:
    interval = int(os.getenv("SANCTUM_INTERVAL_SEC", "300"))
    run_once = os.getenv("SANCTUM_RUN_ONCE", "0") == "1"

    logger.info("[sanctum] worker started interval=%ss run_once=%s", interval, run_once)

    while not STOP:
        try:
            logger.info("[sanctum] start")
            res = parse_sanctum()
            logger.info("[sanctum] ok %s", res)
        except Exception:
            logger.exception("[sanctum] ERROR")

        if run_once:
            break

        # сон с возможностью быстро остановиться
        for _ in range(interval):
            if STOP:
                break
            time.sleep(1)

    logger.info("[sanctum] worker stopped")


if __name__ == "__main__":
    main()

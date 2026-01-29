import os
import time
import signal
import logging

from app.stats import parse_bulk_testnet

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("bulk_testnet_worker")

STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True
    logger.info("stop signal received (%s)", signum)


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)


def main():
    interval = int(os.getenv("BULK_TESTNET_INTERVAL_SEC", "300"))

    logger.info("bulk testnet worker started interval=%ss", interval)

    while not STOP:
        try:
            parse_bulk_testnet()
            logger.info("bulk testnet parsed")
        except Exception:
            logger.exception("bulk testnet parse error")

        for _ in range(interval):
            if STOP:
                break
            time.sleep(1)

    logger.info("bulk testnet worker stopped")


if __name__ == "__main__":
    main()

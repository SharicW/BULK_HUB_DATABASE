import os
import sys
import time
import traceback

ROOT = "/app"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.stats import parse_sanctum, parse_solscan  # noqa: E402


def _get_interval() -> int:
    raw = (os.getenv("PARSER_INTERVAL_SEC") or "300").strip()
    try:
        n = int(raw)
        return max(n, 30)
    except Exception:
        return 300


def log(msg: str) -> None:
    print(msg, flush=True)


def main() -> None:
    interval = _get_interval()
    log(f"[loop] started interval={interval}s")

    while True:
        try:
            log("[sanctum] start")
            res = parse_sanctum()
            log(f"[sanctum] ok {res}")
        except Exception:
            log("[sanctum] ERROR")
            traceback.print_exc()

        try:
            log("[solscan] start")
            res = parse_solscan(limit_rows=25)
            log(f"[solscan] ok {res}")
        except Exception:
            log("[solscan] ERROR")
            traceback.print_exc()

        log(f"[loop] sleep {interval}s")
        time.sleep(interval)


if __name__ == "__main__":
    main()

import os
import sys
import time
import traceback

ROOT = "/app"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.stats import parse_sanctum, parse_solscan


def _get_interval() -> int:
    raw = os.getenv("PARSER_INTERVAL_SEC", "300").strip()
    try:
        val = int(raw)
        return max(val, 30)
    except Exception:
        return 300


def _log(msg: str) -> None:
    print(msg, flush=True)


def main() -> None:
    interval = _get_interval()
    _log(f"[loop] started. interval={interval}s")

    while True:
        # Sanctum
        try:
            _log("[sanctum] start")
            res = parse_sanctum()
            _log(f"[sanctum] ok: {res}")
        except Exception:
            _log("[sanctum] ERROR")
            traceback.print_exc()

        # Solscan
        try:
            _log("[solscan] start")
            res = parse_solscan(limit_rows=25)
            _log(f"[solscan] ok: {res}")
        except Exception:
            _log("[solscan] ERROR")
            traceback.print_exc()

        _log(f"[loop] sleep {interval}s")
        time.sleep(interval)


if name == "main":
    main()

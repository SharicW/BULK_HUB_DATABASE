import time
from app.stats import parse_sanctum, parse_solscan

INTERVAL = int(time.getenv("PARSER_INTERVAL_SEC", "300"))

if name == "main":
    while True:
        try:
            print("Sanctum:", parse_sanctum())
        except Exception as e:
            print("Sanctum error:", e)

        try:
            print("Solscan:", parse_solscan(limit_rows=25))
        except Exception as e:
            print("Solscan error:", e)

        print("Sleeping", INTERVAL, "sec")
        time.sleep(INTERVAL)

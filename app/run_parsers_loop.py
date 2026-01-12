import os
import time
from app.stats import parse_sanctum, parse_solscan

INTERVAL = int(os.getenv("PARSER_INTERVAL_SEC", "300"))

if name == "main":
    while True:
        try:
            print("Sanctum...")
            print(parse_sanctum())
        except Exception as e:
            print("Sanctum error:", e)

        try:
            print("Solscan...")
            print(parse_solscan(limit_rows=25))
        except Exception as e:
            print("Solscan error:", e)

        print(f"Sleep {INTERVAL}s...")
        time.sleep(INTERVAL)

"""
Run inside Coolify container terminal:
  python seed_globe.py
"""
import asyncio, os, uuid

import asyncpg

FAKE_USERS = [
    ("US", "New York",        40.7128,  -74.0060),
    ("US", "Los Angeles",     34.0522, -118.2437),
    ("US", "Chicago",         41.8781,  -87.6298),
    ("US", "Houston",         29.7604,  -95.3698),
    ("US", "Miami",           25.7617,  -80.1918),
    ("GB", "London",          51.5074,   -0.1278),
    ("GB", "Manchester",      53.4808,   -2.2426),
    ("DE", "Berlin",          52.5200,   13.4050),
    ("DE", "Munich",          48.1351,   11.5820),
    ("FR", "Paris",           48.8566,    2.3522),
    ("NL", "Amsterdam",       52.3676,    4.9041),
    ("ES", "Madrid",          40.4168,   -3.7038),
    ("IT", "Rome",            41.9028,   12.4964),
    ("PL", "Warsaw",          52.2297,   21.0122),
    ("UA", "Kyiv",            50.4501,   30.5234),
    ("TR", "Istanbul",        41.0082,   28.9784),
    ("RU", "Moscow",          55.7558,   37.6173),
    ("RU", "Saint Petersburg",59.9343,   30.3351),
    ("KZ", "Almaty",          43.2220,   76.8512),
    ("AE", "Dubai",           25.2048,   55.2708),
    ("SA", "Riyadh",          24.7136,   46.6753),
    ("IN", "Mumbai",          19.0760,   72.8777),
    ("IN", "Bangalore",       12.9716,   77.5946),
    ("IN", "Delhi",           28.7041,   77.1025),
    ("CN", "Shanghai",        31.2304,  121.4737),
    ("CN", "Beijing",         39.9042,  116.4074),
    ("CN", "Shenzhen",        22.5431,  114.0579),
    ("SG", "Singapore",        1.3521,  103.8198),
    ("JP", "Tokyo",           35.6762,  139.6503),
    ("JP", "Osaka",           34.6937,  135.5023),
    ("KR", "Seoul",           37.5665,  126.9780),
    ("TH", "Bangkok",         13.7563,  100.5018),
    ("VN", "Ho Chi Minh",     10.8231,  106.6297),
    ("ID", "Jakarta",         -6.2088,  106.8456),
    ("PH", "Manila",          14.5995,  120.9842),
    ("AU", "Sydney",         -33.8688,  151.2093),
    ("AU", "Melbourne",      -37.8136,  144.9631),
    ("NZ", "Auckland",       -36.8485,  174.7633),
    ("BR", "Sao Paulo",      -23.5505,  -46.6333),
    ("BR", "Rio de Janeiro", -22.9068,  -43.1729),
    ("AR", "Buenos Aires",   -34.6037,  -58.3816),
    ("CO", "Bogota",          4.7110,   -74.0721),
    ("MX", "Mexico City",    19.4326,   -99.1332),
    ("CA", "Toronto",        43.6532,   -79.3832),
    ("CA", "Vancouver",      49.2827,  -123.1207),
    ("ZA", "Johannesburg",   -26.2041,   28.0473),
    ("NG", "Lagos",           6.5244,    3.3792),
    ("EG", "Cairo",          30.0444,   31.2357),
    ("KE", "Nairobi",        -1.2921,   36.8219),
    ("MA", "Casablanca",     33.5731,   -7.5898),
]

async def main():
    url = os.environ.get("AUTH_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("Set AUTH_DATABASE_URL env var")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)

    conn = await asyncpg.connect(url)

    inserted = 0
    for country, city, lat, lng in FAKE_USERS:
        uid = uuid.uuid4()
        email = f"seed_{city.lower().replace(' ', '_')}_{uid.hex[:6]}@bulk.seed"

        await conn.execute(
            """
            INSERT INTO auth_users(id, email, password_hash)
            VALUES($1, $2, '$2b$12$placeholderhashdoesnotwork000000000000000000000')
            ON CONFLICT DO NOTHING
            """,
            uid, email,
        )

        await conn.execute(
            """
            INSERT INTO auth_markers(user_id, country, city, lat, lng)
            VALUES($1, $2, $3, $4, $5)
            ON CONFLICT(user_id) DO NOTHING
            """,
            uid, country, city, lat, lng,
        )
        inserted += 1
        print(f"  + {city}, {country}")

    await conn.close()
    print(f"\nDone: {inserted} markers seeded.")

asyncio.run(main())

import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID

import asyncpg
import bcrypt
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr, Field

from app.auth_db import get_pool

router = APIRouter(prefix="", tags=["auth"])

bearer = HTTPBearer(auto_error=False)

JWT_SECRET = os.getenv("JWT_SECRET", "CHANGE_ME_PLEASE")
JWT_ALG = os.getenv("JWT_ALG", "HS256")
JWT_EXPIRE_DAYS = int(os.getenv("JWT_EXPIRE_DAYS", "30"))


MAX_BCRYPT_BYTES = 72


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=200)


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(min_length=1, max_length=200)
    new_password: str = Field(min_length=1, max_length=200)


class UserOut(BaseModel):
    id: str
    email: EmailStr


class AuthResponse(BaseModel):
    token: str
    user: UserOut


class MarkerIn(BaseModel):
    country: str = Field(min_length=1, max_length=120)
    city: str = Field(min_length=1, max_length=120)
    lat: Optional[float] = None
    lng: Optional[float] = None


class MarkerOut(MarkerIn):
    user_id: str


def _pwd_bytes(password: str) -> bytes:
    b = password.encode("utf-8")
    if len(b) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password cannot be empty.",
        )
    if len(b) > MAX_BCRYPT_BYTES:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password too long (max 72 bytes). Use a shorter password.",
        )
    return b


def _hash_password(password: str) -> str:
    pw = _pwd_bytes(password)
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(pw, salt).decode("utf-8")


def _verify_password(password: str, password_hash: str) -> bool:
    try:
        pw = _pwd_bytes(password)
    except HTTPException:
        return False
    try:
        return bcrypt.checkpw(pw, password_hash.encode("utf-8"))
    except Exception:
        return False


def _create_token(user_id: UUID, email: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS)
    payload = {"sub": str(user_id), "email": email, "exp": exp}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)


def _decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


async def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(bearer),
) -> UserOut:
    if not creds or not creds.credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    payload = _decode_token(creds.credentials)
    user_id = payload.get("sub")
    email = payload.get("email")
    if not user_id or not email:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")

    return UserOut(id=user_id, email=email)


@router.post("/auth/login", response_model=AuthResponse)
async def auth_login(body: LoginRequest):
    email = body.email.strip().lower()
    password = body.password

    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, email, password_hash FROM users WHERE email=$1",
            email,
        )


        if row is None:
            password_hash = _hash_password(password)
            try:
                row = await conn.fetchrow(
                    "INSERT INTO users(email, password_hash) VALUES($1, $2) "
                    "RETURNING id, email, password_hash",
                    email,
                    password_hash,
                )
            except asyncpg.UniqueViolationError:

                row = await conn.fetchrow(
                    "SELECT id, email, password_hash FROM users WHERE email=$1",
                    email,
                )

        if row is None:
            raise HTTPException(status_code=500, detail="User create/read failed")

        if not _verify_password(password, row["password_hash"]):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Wrong password")

        token = _create_token(row["id"], row["email"])
        return AuthResponse(token=token, user=UserOut(id=str(row["id"]), email=row["email"]))


@router.get("/auth/me", response_model=UserOut)
async def auth_me(user: UserOut = Depends(get_current_user)):
    return user

@router.post("/auth/change-password")
async def auth_change_password(body: ChangePasswordRequest, user: UserOut = Depends(get_current_user)):
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT password_hash FROM users WHERE id=$1",
            UUID(user.id),
        )
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

        if not _verify_password(body.current_password, row["password_hash"]):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Wrong password")

        new_hash = _hash_password(body.new_password)
        await conn.execute(
            "UPDATE users SET password_hash=$1 WHERE id=$2",
            new_hash,
            UUID(user.id),
        )

    return {"ok": True}


@router.post("/auth/logout")
async def auth_logout(user: UserOut = Depends(get_current_user)):

    return {"ok": True}



@router.post("/markers", response_model=MarkerOut)
async def save_marker(body: MarkerIn, user: UserOut = Depends(get_current_user)):
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO markers(user_id, country, city, lat, lng)
            VALUES($1, $2, $3, $4, $5)
            ON CONFLICT(user_id) DO UPDATE
              SET country=EXCLUDED.country,
                  city=EXCLUDED.city,
                  lat=EXCLUDED.lat,
                  lng=EXCLUDED.lng,
                  updated_at=now()
            RETURNING user_id, country, city, lat, lng
            """,
            UUID(user.id),
            body.country.strip(),
            body.city.strip(),
            body.lat,
            body.lng,
        )

    return MarkerOut(
        user_id=str(row["user_id"]),
        country=row["country"],
        city=row["city"],
        lat=row["lat"],
        lng=row["lng"],
    )


@router.get("/markers/me", response_model=Optional[MarkerOut])
async def get_my_marker(user: UserOut = Depends(get_current_user)):
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT user_id, country, city, lat, lng FROM markers WHERE user_id=$1",
            UUID(user.id),
        )

    if not row:
        return None

    return MarkerOut(
        user_id=str(row["user_id"]),
        country=row["country"],
        city=row["city"],
        lat=row["lat"],
        lng=row["lng"],
    )


@router.get("/markers", response_model=list[MarkerOut])
async def list_markers(limit: int = 500):
    limit = max(1, min(limit, 2000))
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id, country, city, lat, lng FROM markers ORDER BY updated_at DESC LIMIT $1",
            limit,
        )

    return [
        MarkerOut(
            user_id=str(r["user_id"]),
            country=r["country"],
            city=r["city"],
            lat=r["lat"],
            lng=r["lng"],
        )
        for r in rows
    ]

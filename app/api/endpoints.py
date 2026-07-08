from datetime import datetime, timezone
from random import randint
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException
from kazoo.exceptions import KazooException
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session, select
from starlette import status
from starlette.responses import RedirectResponse
import uuid

from app.core.redis import redis_client, redis_safe_get, redis_safe_set
from app.core.zookeeper import ZooKeeperTokenManager
from app.db.session import get_db
from app.models.url_models import URLs
from app.schemas.url_schema import ShortenRequest, ShortenResponse
import base62

router = APIRouter(prefix="/api/v1")

# Constants for configuration and cache behavior
ZOOKEEPER_HOSTS = "127.0.0.1:2181"
DEFAULT_BASE_URL = "http://localhost:8000/api/v1"
MAX_SHORT_CODE_LENGTH = 8
NEGATIVE_CACHE_VALUE = "404_NOT_FOUND"
NEGATIVE_CACHE_TTL_SECONDS = 300  # 5 minutes
CACHE_TTL_SECONDS = 86400  # 24 hours
CACHE_JITTER_MIN_SECONDS = 0
CACHE_JITTER_MAX_SECONDS = 3600  # 60 minutes

zk_manager = ZooKeeperTokenManager(zk_hosts=ZOOKEEPER_HOSTS)

@router.post("/shorten", status_code=status.HTTP_201_CREATED, response_model=ShortenResponse)
def shorten_url(req: ShortenRequest, db: Annotated[Session, Depends(get_db)]):

    try:
        numeric_id = zk_manager.get_next_id()
        short_code = base62.encode(numeric_id)
    
        new_entry = URLs(
            id=uuid.uuid4(),
            short_code=short_code,
            long_text=req.long_url.unicode_string(),
            expires_at=req.expires_at
        )
    
        db.add(new_entry)
        db.commit()
        db.refresh(new_entry)
    
        return ShortenResponse(
            short_url=f"{DEFAULT_BASE_URL}/{short_code}",
            short_code=new_entry.short_code,
            long_url=new_entry.long_text,
            created_at=new_entry.created_at,
            expires_at=new_entry.expires_at
        )
        
    except KazooException :
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    except SQLAlchemyError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

@router.get("/{short_code}")
def redirect_url(short_code: str, db: Annotated[Session, Depends(get_db)]) -> RedirectResponse:
    #  Early validation of maximum short code length immediately rejects malformed requests, protecting Redis and PostgreSQL from unnecessary I/O overhead.
    if len(short_code) > MAX_SHORT_CODE_LENGTH:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    
    cached_url = redis_safe_get(redis_client, short_code)

    if cached_url:
        #  Negative caching ("404_NOT_FOUND") prevents cache-penetration DoS attacks on non-existent short codes, trading off a short 5-minute propagation delay if the code is created soon after.
        if cached_url == NEGATIVE_CACHE_VALUE:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return RedirectResponse(url=cached_url, status_code=status.HTTP_302_FOUND)

    statement = select(URLs).where(URLs.short_code == short_code)
    entry = db.exec(statement).first()

    now = datetime.now(timezone.utc)
    expires_at = entry.expires_at if entry else None
    #  Normalizing naive timestamps db to UTC to allow comparisons, since python can't compare timezone-naive and timezone-aware
    if expires_at and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if not entry or (expires_at and expires_at <= now):
        # Negative caching to protect against abuse with random non existent short_code
        redis_safe_set(redis_client, short_code, NEGATIVE_CACHE_VALUE, ex=NEGATIVE_CACHE_TTL_SECONDS)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


    #  Adding randomized jitter (up to 60 minutes) to the 24-hour Redis TTL prevents cache stampedes when many popular keys expire simultaneously.
    jitter = randint(CACHE_JITTER_MIN_SECONDS, CACHE_JITTER_MAX_SECONDS)
    if expires_at:
        ttl = min(int((expires_at - now).total_seconds()), CACHE_TTL_SECONDS + jitter)
    else:
        ttl = CACHE_TTL_SECONDS + jitter
        
    redis_safe_set(redis_client, short_code, entry.long_text, ex=ttl)

    return RedirectResponse(url=entry.long_text, status_code=status.HTTP_302_FOUND)

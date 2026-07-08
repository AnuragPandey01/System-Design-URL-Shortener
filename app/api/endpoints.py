from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
from starlette import status
from starlette.responses import RedirectResponse
import redis
import uuid

from core.zookeeper import ZooKeeperTokenManager
from db.session import get_db
from models.url_models import URLs
from schemas.url_schema import ShortenRequest, ShortenResponse
import base62

router = APIRouter(prefix="/api/v1")

zk_manager = ZooKeeperTokenManager(zk_hosts="127.0.0.1:2181")
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

@router.post("/shorten", status_code=status.HTTP_201_CREATED, response_model=ShortenResponse)
def shorten_url(req: ShortenRequest, db: Annotated[Session, Depends(get_db)]):
    numeric_id = zk_manager.get_next_id()
    short_code = base62.encode(numeric_id)

    new_entry = URLs(
        id=uuid.uuid4(),
        short_code=short_code,
        long_text=req.long_url,
        expires_at=req.expires_at
    )

    db.add(new_entry)
    db.commit()
    db.refresh(new_entry)

    return ShortenResponse(
        short_url=f"http://localhost:8000/api/v1/{short_code}",
        short_code=new_entry.short_code,
        long_url=new_entry.long_text,
        created_at=new_entry.created_at,
        expires_at=new_entry.expires_at
    )

@router.get("/{short_code}")
def redirect_url(short_code: str, db: Annotated[Session, Depends(get_db)]):
    cached_url = redis_client.get(short_code)

    if cached_url:
        print("\ncache-hit!!\n")
        return RedirectResponse(url=cached_url, status_code=302)

    statement = select(URLs).where(URLs.short_code == short_code)
    entry = db.exec(statement).first()

    if not entry:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    redis_client.set(short_code, entry.long_text, ex=3600)

    return RedirectResponse(url=entry.long_text, status_code=status.HTTP_302_FOUND)

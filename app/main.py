from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.exceptions import HTTPException
from kazoo.recipe.queue import uuid
from sqlmodel import Session, select
from starlette import status
from starlette.responses import RedirectResponse
import uvicorn
from core.zookeeper import ZooKeeperTokenManager
from db.session import get_db
from models.url_models import URLs
from schemas.url_schema import ShortenRequest, ShortenResponse
import base62

app = FastAPI()

zk_manager = ZooKeeperTokenManager(zk_hosts="127.0.0.1:2181")

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/api/v1/shorten", status_code=status.HTTP_201_CREATED)
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
        short_url=f"http://localhost:8000/{short_code}",
        short_code=new_entry.short_code,
        long_url=new_entry.long_text,
        created_at=new_entry.created_at,
        expires_at=new_entry.expires_at
    )

@app.get("/api/v1/{short_code}")
def redirect_url(short_code: str, db: Annotated[Session, Depends(get_db)]):

    statement = select(URLs).where(URLs.short_code == short_code)
    entry = db.exec(statement).first()

    if not entry:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    return RedirectResponse(url=entry.long_text, status_code=status.HTTP_302_FOUND)

    

if __name__ == "__main__":
    uvicorn.run("main:app",port=8000,reload=True)
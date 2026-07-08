from datetime import datetime

from pydantic import BaseModel

class ShortenRequest(BaseModel):
    long_url: str
    expires_at: datetime


class ShortenResponse(BaseModel):
    short_url: str
    short_code: str
    long_url: str
    created_at: datetime
    expires_at: datetime
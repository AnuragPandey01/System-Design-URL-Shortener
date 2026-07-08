from kazoo.recipe.queue import uuid
from sqlmodel import Field, SQLModel
from datetime import datetime, timezone

class URLs(SQLModel, table=True):
    id : uuid.UUID = Field(primary_key=True)
    short_code : str = Field(max_length=8,index=True,unique=True)
    long_text : str
    click_count : int = Field(default=0)
    expires_at : datetime
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
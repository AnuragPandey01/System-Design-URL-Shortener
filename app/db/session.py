from sqlmodel import Session, create_engine

from core.config import config as settings

SQLALCHEMY_DATABASE_URL = f'postgresql+psycopg://{settings.database_username}:{settings.database_password}@{settings.database_hostname}:{settings.database_port}/{settings.database_name}'

engine = create_engine(SQLALCHEMY_DATABASE_URL)


def get_db():
    with Session(engine) as session:
            yield session
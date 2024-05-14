
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.orm import DeclarativeBase, session, sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select, func, text, JSON
from typing import Dict, Any


class Settings(BaseSettings):
    DB_ADDR: str
    DB_PORT: int
    DB_USER: str
    DB_PASSWORD: str
    DB_DATABASE: str

    @property
    def database_url_asyncpg(self):
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_ADDR}:{self.DB_PORT}/{self.DB_DATABASE}"

    @property
    def database_url_psycopg(self):
        return f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_ADDR}:{self.DB_PORT}/{self.DB_DATABASE}"

    model_config = SettingsConfigDict(env_file="../.env")


settings = Settings()


class DecBase(DeclarativeBase):
    type_annotation_map = {
        Dict[str, Any]: JSON
    }


engine = create_engine(
    url=settings.database_url_psycopg,
    echo=False,
)

async_engine = create_engine(
    url=settings.database_url_asyncpg,
    echo=False,
)

session_factory = sessionmaker(engine)






import os

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from DATABASE.FR import *
from client import ApiClient
from enums import DatabaseUniqueColumns

engine = create_async_engine(os.getenv("DATABASE_URL_API"), echo=False)

async_session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

HEADERS = {
    "Authorization": f"Bearer {os.getenv("FR_API_KEY")}",
    "API Version": "v1",
    "Accept": "application/json"
}

BASE_URL = "https://fr24api.flightradar24.com/api"


async def db_session(_async_session: async_session, **kwargs):
    async with async_session() as session:
        if "stmt" in kwargs:
            return await session.execute(kwargs["stmt"])
        return ApiClient(session, **kwargs)




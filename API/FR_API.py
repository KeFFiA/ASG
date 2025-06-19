import os

import dotenv
import requests
# from sqlalchemy import select
# from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
#
# from DATABASE.FR import *
# from client import ApiClient
# from enums import DatabaseUniqueColumns

# engine = create_async_engine(os.getenv("DATABASE_URL_API"), echo=False)
#
# async_session = async_sessionmaker(
#     bind=engine,
#     class_=AsyncSession,
#     expire_on_commit=False,
# )

dotenv.load_dotenv()

HEADERS = {
    "Authorization": f"Bearer {os.getenv("FR_API_KEY")}",
    "Accept-Version": "v1",
    "Accept": "application/json"
}

BASE_URL = "https://fr24api.flightradar24.com/api"


# async def db_session(_async_session: async_session, **kwargs):
#     async with async_session() as session:
#         if "stmt" in kwargs:
#             return await session.execute(kwargs["stmt"])
#         return ApiClient(session, **kwargs)


result = requests.get(url=f"{BASE_URL}/flight-summary/full", headers=HEADERS, params={
    'flight_datetime_from': '2025-04-10 00:00:00',
    'flight_datetime_to': '2025-04-10 23:59:59',
    'airports': 'inbound:RKT',
    'limit': 20000
}
                      )
if result.status_code == 200:
    print(result.json())
else:
    print(result.status_code)
    print(result.json())


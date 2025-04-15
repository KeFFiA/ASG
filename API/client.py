import json
import os
import ssl
from enum import Enum
from typing import Type, Any

import aiohttp
from dotenv import load_dotenv
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from Utills.Logger import logger
from enums import ICAOEndpoints

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

load_dotenv()

ICAO_API_KEY = os.getenv("ICAO_API_KEY")
BASE_URL = "https://applications.icao.int/dataservices/api"


class ICAOApiClient:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.headers = {"Accept": "application/json"}

    async def _fetch(self, endpoint: str, params: dict) -> list[dict]:
        params["api_key"] = ICAO_API_KEY
        params["format"] = "json"
        params.pop("callback", None)

        url = f"{BASE_URL}/{endpoint}"
        logger.info(f"Sending request to: {url} with params: {params}")

        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as client:
                async with client.get(url, params=params, headers=self.headers) as response:
                    response.raise_for_status()
                    text = await response.text()
                    json_data = json.loads(text)
                    logger.info(f"✅ Received response from {endpoint}: {len(json_data)} records")
                    return json_data
        except aiohttp.ClientResponseError as e:
            logger.error(f"❌ HTTP error [{e.status}] for {url}: {e.message}")
            raise
        except Exception as e:
            logger.exception(f"❌ Unexpected error while fetching data from {url}: {e}")
            raise

    async def _save_to_db(self, data: list[dict], model: Type[Any], conflict_enums: Enum):
        if not data:
            logger.warning("⚠️ No data to save to database.")
            return
        try:
            corrected_data = []
            for item in data:
                corrected_item = {}
                for key, value in item.items():
                    corrected_key = key.replace(" ", "")
                    corrected_item[corrected_key] = value
                corrected_data.append(corrected_item)

            conflict_columns = conflict_enums.value
            update_columns = [col.replace(" ", "") for col in corrected_data[0].keys()
                              if col not in conflict_columns]

            stmt = insert(model).values(corrected_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_={col: stmt.excluded[col] for col in update_columns}
            )

            if not self.session.in_transaction():
                async with self.session.begin():
                    await self.session.execute(stmt)
                    await self.session.commit()
            else:
                await self.session.execute(stmt)
                await self.session.commit()

            logger.info(f"Successfully saved {len(data)} records to {model.__name__}")
        except Exception as e:
            logger.exception(f"❌ Error while saving to {model.__name__}: {e}")
            raise

    async def fetch_and_store(self, endpoint: ICAOEndpoints, model: Type[Any], conflict_enums: Enum, **kwargs):
        logger.debug(f"Fetching and storing data for endpoint: {endpoint}")
        data = await self._fetch(endpoint.value, kwargs)
        await self._save_to_db(data, model, conflict_enums)
